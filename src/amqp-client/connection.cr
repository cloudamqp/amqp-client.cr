require "socket"
require "openssl"
require "amq-protocol"
require "./errors"
require "./channel"
require "../amqp-client"

class AMQP::Client
  class Connection
    LOG = AMQP::Client::LOG.for(self)

    getter channel_max, frame_max, log
    getter? closed = false
    @closing_frame : Frame::Connection::Close?
    @reply_frames = ::Channel(Frame).new

    def initialize(@io : UNIXSocket | TCPSocket | OpenSSL::SSL::Socket::Client | WebSocketIO,
                   @channel_max : UInt16, @frame_max : UInt32, @heartbeat : UInt16)
      spawn read_loop, name: "AMQP::Client#read_loop", same_thread: true
    end

    @channels = Hash(UInt16, Channel).new

    def channel(id : Int? = nil)
      if id
        raise "channel_max reached" if id > @channel_max
        if ch = @channels.fetch(id, nil)
          return ch
        else
          ch = @channels[id] = Channel.new(self, id)
          return ch.open
        end
      end
      1_u16.upto(@channel_max) do |i|
        next if @channels.has_key? i
        ch = @channels[i] = Channel.new(self, i)
        return ch.open
      end
      raise "channel_max reached"
    end

    def channel(&blk : Channel -> _)
      ch = channel
      yield ch
    ensure
      ch.try &.close
    end

    @on_close : Proc(UInt16, String, Nil)?

    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    private def read_loop
      io = @io
      loop do
        Frame.from_io(io) do |f|
          LOG.debug { "got #{f.inspect}" }
          case f
          when Frame::Connection::Close
            if on_close = @on_close
              begin
                on_close.call(f.reply_code, f.reply_text)
              rescue ex
                LOG.error(exception: ex) { "Uncaught exception in on_close block" }
              end
            else
              LOG.info { "Connection closed by server: #{f.reply_text} (code #{f.reply_code})" }
            end
            write Frame::Connection::CloseOk.new
            @closing_frame = f
            return
          when Frame::Connection::CloseOk
            begin
              @reply_frames.send f
            rescue ::Channel::ClosedError
              LOG.debug { "CloseOk ignored by user" }
            end
            return
          when Frame::Connection::Blocked
            LOG.info { "Blocked by server, reason: #{f.reason}" }
            @write_lock.lock
          when Frame::Connection::Unblocked
            LOG.info { "Unblocked by server" }
            @write_lock.unlock
          when Frame::Heartbeat
            write f
          else
            if ch = @channels.fetch(f.channel, nil)
              ch.incoming f
            else
              LOG.error { "Channel #{f.channel} not open for frame #{f.inspect}" }
            end

            if f.is_a?(Frame::Channel::Close) || f.is_a?(Frame::Channel::CloseOk)
              @channels.delete f.channel
            end
          end
        end
      rescue ex : IO::Error | OpenSSL::Error
        LOG.error(exception: ex) { "connection closed unexpectedly: #{ex.message}" }
        break
      rescue ex
        LOG.error(exception: ex) { "read_loop exception: #{ex.inspect}" }
        break
      end
    ensure
      @closed = true
      @io.close rescue nil
      @reply_frames.close
      @channels.each_value &.cleanup
      @channels.clear
    end

    @write_lock = Mutex.new

    def write(frame : Frame)
      @write_lock.synchronize do
        unsafe_write(frame)
        @io.flush
      end
    end

    def unsafe_write(frame : Frame)
      if @closed
        if f = @closing_frame
          raise ClosedException.new(f)
        else
          return
        end
      end
      begin
        @io.write_bytes frame, ::IO::ByteFormat::NetworkEndian
      rescue ex
        raise Error.new(cause: ex)
      end
      LOG.debug { "sent #{frame.inspect}" }
    end

    def with_lock(&blk : self -> _)
      @write_lock.synchronize do
        yield self
        @io.flush
      end
    end

    def close(msg = "", no_wait = false)
      return if @closed
      LOG.debug { "Closing connection" }
      write Frame::Connection::Close.new(200_u16, msg, 0_u16, 0_u16)
      return if no_wait
      while frame = @reply_frames.receive?
        if frame.as?(Frame::Connection::CloseOk)
          LOG.debug { "Server confirmed close" }
          return
        end
      end
      LOG.debug { "Server didn't confirm close" }
    rescue ex : IO::Error
      LOG.info { "Socket already closed, can't send close frame" }
    ensure
      @closed = true
      @reply_frames.close
      @io.close rescue nil
      @channels.each_value &.cleanup
      @channels.clear
    end

    def self.start(io : UNIXSocket | TCPSocket | OpenSSL::SSL::Socket::Client | WebSocketIO,
                   user, password, vhost,
                   channel_max, frame_max, heartbeat, name : String?)
      io.read_timeout = 15
      io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
      io.flush
      Frame.from_io(io) { |f| f.as?(Frame::Connection::Start) || raise UnexpectedFrame.new(f) }

      props = Arguments.new
      props["connection_name"] = name if name
      props["product"] = "amqp-client.cr"
      props["platform"] = "Crystal"
      props["version"] = AMQP::Client::VERSION
      capabilities = Arguments.new
      capabilities["publisher_confirms"] = true
      capabilities["exchange_exchange_bindings"] = true
      capabilities["basic.nack"] = true
      capabilities["per_consumer_qos"] = true
      capabilities["authentication_failure_close"] = true
      capabilities["consumer_cancel_notify"] = true
      capabilities["connection.blocked"] = true
      props["capabilities"] = capabilities
      user = URI.decode_www_form(user)
      password = URI.decode_www_form(password)
      response = "\u0000#{user}\u0000#{password}"
      io.write_bytes(Frame::Connection::StartOk.new(props, "PLAIN", response, ""),
        IO::ByteFormat::NetworkEndian)
      io.flush
      tune = Frame.from_io(io) do |f|
        case f
        when Frame::Connection::Tune then next f
        when Frame::Connection::Close
          raise Connection::ClosedException.new(f.as(Frame::Connection::Close))
        else
          raise UnexpectedFrame.new(f)
        end
      end
      channel_max = tune.channel_max.zero? ? channel_max : Math.min(tune.channel_max, channel_max)
      frame_max = tune.frame_max.zero? ? frame_max : Math.min(tune.frame_max, frame_max)
      io.write_bytes Frame::Connection::TuneOk.new(channel_max: channel_max,
        frame_max: frame_max,
        heartbeat: heartbeat),
        IO::ByteFormat::NetworkEndian
      io.write_bytes Frame::Connection::Open.new(vhost), IO::ByteFormat::NetworkEndian
      io.flush
      Frame.from_io(io) do |f|
        case f
        when Frame::Connection::OpenOk then next
        when Frame::Connection::Close
          raise Connection::ClosedException.new(f.as(Frame::Connection::Close))
        else
          raise UnexpectedFrame.new(f)
        end
      end

      Connection.new(io, channel_max, frame_max, heartbeat)
    rescue ex
      case ex
      when IO::EOFError
        raise Connection::ClosedException.new("Connection closed by server", ex)
      when Connection::ClosedException
        io.write_bytes(Frame::Connection::CloseOk.new, IO::ByteFormat::NetworkEndian) rescue nil
      else nil
      end
      io.close rescue nil
      raise ex
    ensure
      io.read_timeout = nil
    end
  end
end
