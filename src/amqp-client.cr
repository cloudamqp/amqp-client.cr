require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "logger"
require "./amqp-client/*"

class AMQP::Client
  def self.start(url : String | URI, log_level = Logger::WARN, &blk : AMQP::Client::Connection -> _)
    conn = self.new(url, log_level).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.start(host = "localhost", port = 5672, vhost = "/",
                 user = "guest", password = "guest", tls = false,
                 channel_max = UInt16::MAX, frame_max = 131_072_u32, heartbeat = 0_u16,
                 verify_mode = OpenSSL::SSL::VerifyMode::PEER,
                 log_level = Logger::WARN, &blk : AMQP::Client::Connection -> _)
    conn = self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, verify_mode, log_level).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.new(url : String, log_level = Logger::WARN)
    uri = URI.parse(url)
    self.new(uri, log_level)
  end

  def self.new(uri : URI, log_level = Logger::WARN)
    tls = uri.scheme == "amqps"
    host = uri.host.to_s.empty? ? "localhost" : uri.host.to_s
    port = uri.port || (tls ? 5671 : 5672)
    vhost =
      if (path = uri.path) && path.bytesize > 1
        URI.decode_www_form(path[1..-1])
      else
        "/"
      end
    user = uri.user || "guest"
    password = uri.password || "guest"
    arguments = uri.query.try(&.split("&").map(&.split("=")).to_h) || Hash(String, String).new
    heartbeat = arguments.fetch("heartbeat", 0_u16).to_u16
    frame_max = arguments.fetch("frame_max", 131_072_u32).to_u32
    channel_max = arguments.fetch("channel_max", UInt16::MAX).to_u16
    verify_mode = case arguments.fetch("verify", "").downcase
                  when "none" then OpenSSL::SSL::VerifyMode::NONE
                  else             OpenSSL::SSL::VerifyMode::PEER
                  end
    self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, verify_mode, log_level)
  end


  def initialize(@host = "localhost", @port = 5672, @vhost = "/", @user = "guest", @password = "guest",
                 @tls = false, @channel_max = UInt16::MAX, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 @verify_mode = OpenSSL::SSL::VerifyMode::PEER, log_level = Logger::WARN)
    @log = Logger.new(STDERR, level: log_level)
    @log.progname = "amqp-client.cr"
  end
    
  def connect : Connection
    if @host.starts_with? '/'
      socket = connect_unix
      Connection.start(socket, @log, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat)
    elsif @tls
      socket = connect_tls(connect_tcp)
      Connection.start(socket, @log, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat)
    else
      socket = connect_tcp
      Connection.start(socket, @log, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat)
    end
  end

  # High-level subscribe mthod
  # Will automatically reconnect
  # ```
  # amqp = AMQP::Client.new
  # amqp.subscribe("my_queue", [{ "amq.topic", "mykey" }]) do |msg|
  #   process(msg)
  # end
  # ```
  def subscribe(queue_name, bindings = Array(Tuple(String, String)).new, args = Arguments.new)
    loop do
      c = connection
      ch = c.channel
      q = begin
            ch.queue(queue)
          rescue
            ch = c.channel
            ch.queue(queue, passive: true)
          end
      bindings.each do |exchange, routing_key|
        q.bind(exchange, routing_key)
      end
      ch.basic_consume(q[:name], no_ack: false, block: true, args: args) do |msg|
        begin
          yield msg
        rescue
          msg.nack requeue: true
        else
          msg.ack
        end
      end
    rescue ex
      @log.warn { "consumer retrying: #{ex.inspect}" }
    end
  end

  # High-level publish method
  # Messages are processed in an internal queue
  # Will automatically reconnect if disconnected
  # ```
  # amqp = AMQP::Client.new
  # amqp.publish "mydata".to_slice, "amq.topic", "routing.key"
  # ```
  def publish(bytes : Bytes, exchange, routing_key = "", props = Properties.new)
    @publish_loop ||= spawn publish_loop, name: "AMQP::Client#publish_loop"
    @publish_queue.send PubMsg.new(bytes, exchange, routing_key, props)
  end

  def publish(str : String, exchange, routing_key = "", props = Properties.new)
    publish(str.to_slice, exchange, routing_key, props)
  end

  record PubMsg, bytes : Bytes, exchange : String, routing_key : String, props : Properties

  @publish_queue = ::Channel(Message).new(128)

  private def publish_loop
    loop do
      msg = @publish_queue.receive? || break
      c = connection
      ch = c.channel(c.channel_max)
      ch.basic_publish(msg.bytes, msg.exchange, msg.routing_key,
                       props: msg.props)
    rescue ex
      @log.warn { "publish_loop retrying: #{ex.inspect}" }
      @publish_queue.send msg
    end
  end

  @connection : Connection?
  @connection_lock = Mutex.new

  private def connection : Connection
    @connection_lock.synchronize do
      return @connection if @connection && !@connection.closed?
      @connection = connect
    end
  end

  private def connect_tcp
    socket = TCPSocket.new(@host, @port, connect_timeout: 5)
    socket.keepalive = true
    socket.tcp_nodelay = false
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.sync = false
    socket.read_buffering = true
    socket.write_timeout = 15
    socket
  end

  private def connect_tls(socket)
    ctx = OpenSSL::SSL::Context::Client.new
    ctx.verify_mode = @verify_mode
    tls_socket = OpenSSL::SSL::Socket::Client.new(socket, ctx, sync_close: true, hostname: @host)
    socket.sync = true
    socket.read_buffering = false
    tls_socket.sync = false
    tls_socket.read_buffering = true
    tls_socket
  end

  private def connect_unix
    UNIXSocket.new(@host).tap do |socket|
      socket.sync = false
      socket.read_buffering = true
      socket.write_timeout = 15
    end
  end

  alias Frame = AMQ::Protocol::Frame
  alias Arguments = AMQ::Protocol::Table
  alias Properties = AMQ::Protocol::Properties
  class UnexpectedFrame < Exception
    def initialize
      super
    end
    def initialize(frame : Frame)
      super(frame.inspect)
    end
  end
end
