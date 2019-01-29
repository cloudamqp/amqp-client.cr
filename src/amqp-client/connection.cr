require "socket"
require "openssl"
require "logger"
require "amq-protocol"
require "./channel"

class AMQP::Client
  class Connection
    getter frame_max, log

    def initialize(@io : TCPSocket | OpenSSL::SSL::Socket::Client, @log : Logger, @channel_max : UInt16, @frame_max : UInt32)
      spawn read_loop, name: "AMQP::Client#read_loop"
    end

    @channels = Hash(UInt16, Channel).new

    def channel(id : UInt16? = nil)
      if id
        raise "channel_max reached" if @channel_max < id
        return @channels[id] if @channels.has_key? id
        return @channels[id] = Channel.new(self, id).open
      end
      1_u16.upto(@channel_max) do |i|
        next if @channels.has_key? i
        ch = @channels[i] = Channel.new(self, i)
        ch.open
        return ch
      end
      raise "channel_max reached"
    end

    def channel(&blk : Channel -> _)
      ch = channel
      yield ch
      ch.close
    end

    @on_close : Proc((UInt16, String), Nil)? 

    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    private def read_loop
      loop do
        AMQ::Protocol::Frame.from_io(@io) do |f|
          @log.debug "got #{f.inspect}"
          case f
          when AMQ::Protocol::Frame::Connection::Close
            @log.error("Connection closed by server: #{f.inspect}") unless @on_close
            begin
              @on_close.try &.call(f.reply_code, f.reply_text)
            rescue ex
              @log.error "Uncaught exception in on_close block: #{ex.inspect_with_backtrace}"
            end
            write AMQ::Protocol::Frame::Connection::CloseOk.new
            next false
          when AMQ::Protocol::Frame::Connection::CloseOk
            next false
          when AMQ::Protocol::Frame::Heartbeat
            write f
          else
            if @channels.has_key? f.channel
              @channels[f.channel].incoming f
            else
              @log.error "Channel #{f.channel} not open for frame #{f.inspect}"
            end

            case f
            when AMQ::Protocol::Frame::Connection::Close,
                 AMQ::Protocol::Frame::Connection::CloseOk
              @channels.delete f.channel
            end
          end
          true
        end || break
      rescue ex : IO::Error | Errno
        break
      rescue ex
        @log.error "read_loop exception: #{ex.inspect}"
        break
      end
      @io.close
    rescue ex : Errno
    end

    def write(frame, flush = true)
      @io.write_bytes frame, ::IO::ByteFormat::NetworkEndian
      @io.flush if flush
      @log.info "sent #{frame.inspect}"
    end

    def close(msg = "Connection closed")
      @channels.each_value &.cleanup
      @channels.clear
      @log.info("Closing connection")
      write AMQ::Protocol::Frame::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
    rescue ex : Errno | IO::Error
      @log.info("Socket already closed, can't send close frame")
    end

    def closed?
      @io.closed?
    end
  end
end
