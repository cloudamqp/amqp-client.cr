require "socket"
require "openssl"
require "amq-protocol"
require "./errors"
require "./channel"
require "../amqp-client"

class AMQP::Client
  class Connection
    private Log = ::Log.for(self)

    @reply_frames = ::Channel(Frame).new
    getter closing_frame : Frame::Connection::Close?
    getter channel_max, frame_max
    getter? closed = false
    getter? blocked = false
    @connection_id : String

    protected def initialize(@io : UNIXSocket | TCPSocket | OpenSSL::SSL::Socket::Client | WebSocketIO,
                             @channel_max : UInt16, @frame_max : UInt32, @heartbeat : UInt16)
      @connection_id = connection_identifier(@io)
      spawn read_loop, name: "AMQP::Client#read_loop"
    end

    private def connection_identifier(io) : String
      case io
      when TCPSocket
        io.local_address.port.to_s
      when OpenSSL::SSL::Socket::Client
        # SSL sockets wrap a TCP socket, get the port from the underlying socket
        if io.io.is_a?(TCPSocket)
          io.io.as(TCPSocket).local_address.port.to_s
        else
          object_id.to_s
        end
      when UNIXSocket
        # For UNIX sockets, we could use the path, but object_id is simpler and unique
        object_id.to_s
      else
        # For WebSocketIO and other types, use object_id
        object_id.to_s
      end
    end

    @channels = Hash(UInt16, Channel).new
    @channels_lock = Mutex.new

    # Opens a channel
    def channel(id : Int? = nil)
      ch = nil
      @channels_lock.synchronize do
        if id
          raise "channel_max reached" if @channel_max.positive? && id > @channel_max
          if ch = @channels.fetch(id, nil)
            return ch
          else
            ch = @channels[id] = Channel.new(self, id)
          end
        end
        range = @channel_max.zero? ? (1_u16..UInt16::MAX) : (1_u16..@channel_max)
        range.each do |i|
          next if @channels.has_key? i
          ch = @channels[i] = Channel.new(self, i)
          break
        end
      end
      raise "channel_max reached" if ch.nil?
      # We must open the channel outside of the lock to avoid deadlocks
      ch.open
    end

    def channel(& : Channel -> _)
      ch = channel
      yield ch
    ensure
      ch.try &.close
    end

    @update_secret_ok = ::Channel(Nil).new

    def update_secret(secret : String, reason : String) : Nil
      write Frame::Connection::UpdateSecret.new(secret, reason)
      @update_secret_ok.receive
    rescue ::Channel::ClosedError
      if f = @closing_frame
        raise ClosedException.new(f)
      end
    end

    @on_close : Proc(UInt16, String, Nil)?

    # Callback that's called if the `Connection` is closed by the server
    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    @on_blocked : Proc(String, Nil)?
    @on_unblocked : Proc(Nil)?

    # Callback called when server is blocked, first argument is the reason from the server
    def on_blocked(&blk : String ->)
      @on_blocked = blk
    end

    # Callback for when the server is unblocked again
    def on_unblocked(&blk : ->)
      @on_unblocked = blk
    end

    private def read_loop # ameba:disable Metrics/CyclomaticComplexity
      Log.context.set connection_id: @connection_id
      io = @io
      loop do
        Frame.from_io(io) do |f|
          Log.debug { "recv #{f.inspect}" }
          case f
          when Frame::Connection::Close
            process_close(f)
            return
          when Frame::Connection::CloseOk
            begin
              @reply_frames.send f
            rescue ::Channel::ClosedError
              Log.debug { "CloseOk ignored by user" }
            end
            return
          when Frame::Connection::Blocked
            Log.info { "Blocked by server, reason: #{f.reason}" }
            @blocked = true
            @on_blocked.try &.call(f.reason)
          when Frame::Connection::Unblocked
            Log.info { "Unblocked by server" }
            @blocked = false
            @on_unblocked.try &.call
          when Frame::Connection::UpdateSecretOk
            @update_secret_ok.send nil
          when Frame::Heartbeat
            write f
          else
            process_channel_frame(f)
          end
        end
      rescue ex : IO::Error | OpenSSL::Error
        Log.error(exception: ex) { "connection closed unexpectedly: #{ex.message}" }
        break
      rescue ex
        Log.error(exception: ex) { "read_loop exception: #{ex.inspect}" }
        break
      end
    ensure
      @closed = true
      @io.close rescue nil
      @reply_frames.close
      @update_secret_ok.close
      @channels_lock.synchronize do
        @channels.each_value &.cleanup
        @channels.clear
      end
    end

    private def process_close(f)
      Log.context.set connection_id: @connection_id
      if on_close = @on_close
        begin
          on_close.call(f.reply_code, f.reply_text)
        rescue ex
          Log.error(exception: ex) { "Uncaught exception in on_close block" }
        end
      else
        Log.info { "Connection closed by server: #{f.reply_text} (code #{f.reply_code})" }
      end
      begin
        write Frame::Connection::CloseOk.new
      rescue ex
        Log.error(exception: ex) { "Couldn't write CloseOk frame" }
      end
      @closing_frame = f
    end

    private def process_channel_frame(f)
      Log.context.set connection_id: @connection_id
      ch = case f
           when Frame::Channel::Close, Frame::Channel::CloseOk
             @channels_lock.synchronize do
               @channels.delete f.channel
             end
           else
             @channels.fetch(f.channel, nil)
           end
      if ch
        ch.incoming f
      else
        Log.error { "Channel #{f.channel} not open for frame #{f.inspect}" }
      end

      case f
      when Frame::Channel::Close, Frame::Channel::CloseOk
        @channels_lock.synchronize do
          @channels.delete f.channel
        end
      end
    end

    @write_lock = Mutex.new

    # :nodoc:
    def write(frame : Frame)
      @write_lock.synchronize do
        unsafe_write(frame)
        @io.flush
      end
    end

    # :nodoc:
    def unsafe_write(frame : Frame)
      Log.context.set connection_id: @connection_id
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
      Log.debug { "sent #{frame.inspect}" }
    end

    # :nodoc:
    def with_lock(flush = true, & : self -> _)
      @write_lock.synchronize do
        yield self
        @io.flush if flush
      end
    end

    # Close the connection the server.
    #
    # The *reason* might be logged by the server
    def close(reason = "", no_wait = false)
      Log.context.set connection_id: @connection_id
      return if @closed
      Log.debug { "Closing connection" }
      write Frame::Connection::Close.new(200_u16, reason, 0_u16, 0_u16)
      return if no_wait
      while frame = @reply_frames.receive?
        if frame.as?(Frame::Connection::CloseOk)
          Log.debug { "Server confirmed close" }
          return
        end
      end
      Log.debug { "Server didn't confirm close" }
    rescue ex : IO::Error
      Log.info { "Socket already closed, can't send close frame" }
    ensure
      @closed = true
      @reply_frames.close
      @io.close rescue nil
      @channels.each_value &.cleanup
      @channels.clear
    end

    # Connection negotiation
    def self.start(io : UNIXSocket | TCPSocket | OpenSSL::SSL::Socket::Client | WebSocketIO,
                   user, password, vhost, channel_max, frame_max, heartbeat, connection_information,
                   name = File.basename(PROGRAM_NAME))
      io.read_timeout = 60.seconds
      connection_information.name ||= name
      start(io, user, password, connection_information)
      channel_max, frame_max, heartbeat = tune(io, channel_max, frame_max, heartbeat)
      open(io, vhost)
      self.new(io, channel_max, frame_max, heartbeat)
    rescue ex
      case ex
      when IO::EOFError
        raise ClosedException.new("Connection closed by server", ex)
      when ClosedException
        io.write_bytes(Frame::Connection::CloseOk.new, IO::ByteFormat::NetworkEndian) rescue nil
      else nil
      end
      io.close rescue nil
      raise ex
    ensure
      io.read_timeout = nil
    end

    private def self.start(io, user, password, connection_information)
      io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
      io.flush
      Frame.from_io(io) { |f| f.as?(Frame::Connection::Start) || raise Error::UnexpectedFrame.new(f) }
      props = Arguments.new({
        connection_name:  connection_information.name,
        product:          connection_information.product,
        platform:         connection_information.platform,
        product_version:  connection_information.product_version,
        platform_version: connection_information.platform_version,
        capabilities:     Arguments.new({
          "publisher_confirms":           true,
          "exchange_exchange_bindings":   true,
          "basic.nack":                   true,
          "per_consumer_qos":             true,
          "authentication_failure_close": true,
          "consumer_cancel_notify":       true,
          "connection.blocked":           true,
        }),
      })
      user = URI.decode_www_form(user)
      password = URI.decode_www_form(password)
      response = "\u0000#{user}\u0000#{password}"
      io.write_bytes(Frame::Connection::StartOk.new(props, "PLAIN", response, ""),
        IO::ByteFormat::NetworkEndian)
      io.flush
    end

    private def self.tune(io, channel_max, frame_max, heartbeat)
      tune = Frame.from_io(io) do |f|
        case f
        when Frame::Connection::Tune  then f
        when Frame::Connection::Close then raise ClosedException.new(f)
        else                               raise Error::UnexpectedFrame.new(f)
        end
      end

      negotiated_channel_max = tune.channel_max
      if channel_max > 0
        if negotiated_channel_max == 0
          negotiated_channel_max = channel_max
        else
          negotiated_channel_max = Math.min(channel_max, negotiated_channel_max)
        end
      end

      frame_max = tune.frame_max.zero? ? frame_max : Math.min(tune.frame_max, frame_max)
      io.write_bytes Frame::Connection::TuneOk.new(channel_max: negotiated_channel_max,
        frame_max: frame_max,
        heartbeat: heartbeat),
        IO::ByteFormat::NetworkEndian
      {negotiated_channel_max, frame_max, heartbeat}
    end

    private def self.open(io, vhost)
      io.write_bytes Frame::Connection::Open.new(vhost), IO::ByteFormat::NetworkEndian
      io.flush
      Frame.from_io(io) do |f|
        case f
        when Frame::Connection::OpenOk then f
        when Frame::Connection::Close
          raise ClosedException.new(f)
        else
          raise Error::UnexpectedFrame.new(f)
        end
      end
    end
  end
end
