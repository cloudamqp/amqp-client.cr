require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "log"
require "./amqp-client/*"

class AMQP::Client
  private Log         = ::Log.for(self)
  private WS_SCHEMES  = {"ws", "wss", "http", "https"}
  private TLS_SCHEMES = {"amqps", "wss", "https"}
  private SCHEME_PORT = {"amqp" => 5672, "amqps" => 5671, "ws" => 80, "wss" => 443, "http" => 80, "https" => 443}
  # :nodoc:
  AMQP_URL = ENV["AMQP_URL"]?.try { |u| URI.parse(u) }
  # :nodoc:
  AMQP_TLS = TLS_SCHEMES.includes?(AMQP_URL.try(&.scheme)) || false
  # :nodoc:
  AMQP_WS = WS_SCHEMES.includes?(AMQP_URL.try(&.scheme)) || false
  # :nodoc:
  AMQP_HOST = AMQP_URL.try(&.host) || "localhost"
  # :nodoc:
  AMQP_PORT = AMQP_URL.try(&.port) || AMQP_WS ? (AMQP_TLS ? 443 : 80) : (AMQP_TLS ? 5671 : 5672)
  # :nodoc:
  AMQP_USER = AMQP_URL.try(&.user) || "guest"
  # :nodoc:
  AMQP_PASS = AMQP_URL.try(&.password) || "guest"
  # :nodoc:
  AMQP_VHOST = AMQP_URL.try { |u| URI.decode_www_form(u.path[1..-1]) if u.path.bytesize > 1 } || "/"

  alias TLSContext = OpenSSL::SSL::Context::Client | Bool | Nil

  def self.start(url : String | URI, &blk : AMQP::Client::Connection -> _)
    conn = self.new(url).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.start(host = AMQP_HOST, port = AMQP_PORT, vhost = AMQP_VHOST,
                 user = AMQP_USER, password = AMQP_PASS, tls : TLSContext = AMQP_TLS, websocket = AMQP_WS,
                 channel_max = 1024_u16, frame_max = 131_072_u32, heartbeat = 0_u16,
                 verify_mode = OpenSSL::SSL::VerifyMode::PEER, name = nil, &blk : AMQP::Client::Connection -> _)
    conn = self.new(host, port, vhost, user, password, tls, websocket, channel_max, frame_max, heartbeat, verify_mode, name).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.new(url : String)
    uri = URI.parse(url)
    self.new(uri)
  end

  def self.new(uri : URI) # ameba:disable Metrics/CyclomaticComplexity
    tls = TLS_SCHEMES.includes? uri.scheme
    websocket = WS_SCHEMES.includes? uri.scheme
    host = uri.host.to_s.empty? ? "localhost" : uri.host.to_s
    port = uri.port || SCHEME_PORT[uri.scheme]
    vhost = uri.path.bytesize > 1 ? URI.decode_www_form(uri.path[1..-1]) : "/"
    user = uri.user || "guest"
    password = uri.password || "guest"
    Log.debug { "Opening connection to #{host} with arguments #{uri.query_params}" }
    heartbeat = 0_u16
    frame_max = 131_072_u32
    channel_max = 1024_u16
    verify_mode = OpenSSL::SSL::VerifyMode::PEER
    name = File.basename(PROGRAM_NAME)
    buffer_size = 16384
    tcp = TCPConfig.new
    uri.query_params.each do |key, value|
      case key
      when "name"             then name = URI.decode_www_form(value)
      when "heartbeat"        then heartbeat = value.to_u16
      when "frame_max"        then frame_max = value.to_u32
      when "channel_max"      then channel_max = value.to_u16
      when "buffer_size"      then buffer_size = value.to_i
      when "tcp_nodelay"      then tcp.nodelay = true
      when "recv_buffer_size" then tcp.recv_buffer_size = value.to_i
      when "send_buffer_size" then tcp.send_buffer_size = value.to_i
      when "tcp_keepalive"
        ka = value.split(':', 3).map &.to_i
        tcp.keepalive_idle, tcp.keepalive_interval, tcp.keepalive_count = ka
      when "verify"
        verify_mode = OpenSSL::SSL::VerifyMode::NONE if value =~ /^none$/i
      else raise ArgumentError.new("Invalid parameter: #{key}")
      end
    end
    self.new(host, port, vhost, user, password, tls, websocket,
      channel_max, frame_max, heartbeat, verify_mode, name,
      tcp, buffer_size)
  end

  property host, port, vhost, user, websocket, tcp, buffer_size
  property tls : OpenSSL::SSL::Context::Client?

  # record Tune, channel_max = 1024u16, frame_max = 131_072u32, heartbeat = 0u16
  record TCPConfig, nodelay = false, keepalive_idle = 60, keepalive_interval = 10, keepalive_count = 3, send_buffer_size : Int32? = nil, recv_buffer_size : Int32? = nil do
    property nodelay, keepalive_idle, keepalive_interval, keepalive_count, send_buffer_size, recv_buffer_size
  end

  def initialize(@host = AMQP_HOST, @port = AMQP_PORT, @vhost = AMQP_VHOST, @user = AMQP_USER, @password = AMQP_PASS,
                 tls : TLSContext = AMQP_TLS, @websocket = AMQP_WS, @channel_max = 1024_u16, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 verify_mode = OpenSSL::SSL::VerifyMode::PEER, @name : String? = File.basename(PROGRAM_NAME),
                 @tcp = TCPConfig.new, @buffer_size = 16384)
    if tls.is_a? OpenSSL::SSL::Context::Client
      @tls = tls
    elsif tls == true
      @tls = OpenSSL::SSL::Context::Client.new.tap(&.verify_mode = verify_mode)
    end
  end

  # Establish a connection
  def connect : Connection
    if @host.starts_with? '/'
      socket = connect_unix
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    elsif @websocket
      websocket = ::HTTP::WebSocket.new(@host, path: "", port: @port, tls: @tls)
      io = WebSocketIO.new(websocket)
      Connection.start(io, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    elsif ctx = @tls.as? OpenSSL::SSL::Context::Client
      socket = connect_tls(connect_tcp, ctx)
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    else
      socket = connect_tcp
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    end
  rescue ex
    case ex
    when Connection::ClosedException
      # agument the exception with connection details
      raise Connection::ClosedException.new(ex.message, @host, @user, @vhost)
    when Error then raise ex
    else            raise Error.new(ex.message, cause: ex)
    end
  end

  private def connect_tcp
    socket = TCPSocket.new(@host, @port, connect_timeout: 60)
    socket.keepalive = true if @tcp.keepalive_idle.positive?
    socket.tcp_keepalive_idle = @tcp.keepalive_idle if @tcp.keepalive_idle.positive?
    socket.tcp_keepalive_count = @tcp.keepalive_count if @tcp.keepalive_count.positive?
    socket.tcp_keepalive_interval = @tcp.keepalive_interval if @tcp.keepalive_interval.positive?
    socket.tcp_nodelay = true if @tcp.nodelay
    @tcp.recv_buffer_size.try { |v| socket.recv_buffer_size = v }
    @tcp.send_buffer_size.try { |v| socket.send_buffer_size = v }
    set_socket_buffers(socket)
    socket
  end

  private def connect_tls(socket, context)
    OpenSSL::SSL::Socket::Client.new(socket, context, sync_close: true, hostname: @host).tap do |tls_socket|
      set_socket_buffers(tls_socket)
    end
  end

  private def connect_unix
    UNIXSocket.new(@host).tap do |socket|
      set_socket_buffers(socket)
    end
  end

  private def set_socket_buffers(socket)
    if @buffer_size.positive?
      socket.buffer_size = @buffer_size
      socket.sync = false
      socket.read_buffering = true
    else
      socket.sync = true
      socket.read_buffering = false
    end
  end

  # :nodoc:
  alias Frame = AMQ::Protocol::Frame
  # :nodoc:
  alias Arguments = AMQ::Protocol::Table
  # :nodoc:
  alias Properties = AMQ::Protocol::Properties
end
