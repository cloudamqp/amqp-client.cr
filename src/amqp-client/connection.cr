require "socket"
require "openssl"
require "amq-protocol"
require "./channel"

class AMQP::Client
  class Connection
    def initialize(@io : TCPSocket | OpenSSL::SSL::Socket::Client, @log : Logger, @channel_max : UInt16)
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

    private def read_loop
      loop do
        AMQ::Protocol::Frame.from_io(@io) do |f|
          if ch = @channels[f.channel]
            ch.incoming f
          else
            @log.error "No channel open: #{f.inspect}"
          end
        end
      end
    end

    def write(frame, flush = true)
      @io.write_bytes frame, ::IO::ByteFormat::NetworkEndian
      @io.flush if flush
    end

    def close(msg = "Connection closed")
      @channels.each do |id, channel|
        channel.close
      end
      @channels.clear
      write AMQ::Protocol::Frame::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
    rescue Errno
      @log.info("Socket already closed, can't send close frame")
    end

    def closed?
      @io.closed?
    end
  end
end
