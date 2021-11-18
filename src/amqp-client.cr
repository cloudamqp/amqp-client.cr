require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "log"
require "./amqp-client/*"

class AMQP::Client
  private LOG         = ::Log.for(self)
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

  def self.start(url : String | URI, &blk : AMQP::Client::Connection -> _)
    conn = self.new(url).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.start(host = AMQP_HOST, port = AMQP_PORT, vhost = AMQP_VHOST,
                 user = AMQP_USER, password = AMQP_PASS, tls = AMQP_TLS, websocket = AMQP_WS,
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

  def self.new(uri : URI)
    tls = TLS_SCHEMES.includes? uri.scheme
    websocket = WS_SCHEMES.includes? uri.scheme
    host = uri.host.to_s.empty? ? "localhost" : uri.host.to_s
    port = uri.port || SCHEME_PORT[uri.scheme]
    vhost =
      if (path = uri.path) && path.bytesize > 1
        URI.decode_www_form(path[1..-1])
      else
        "/"
      end
    user = uri.user || "guest"
    password = uri.password || "guest"
    arguments = uri.query_params
    LOG.debug { "Opening connection to #{host} with arguments #{arguments}" }
    heartbeat = arguments.fetch("heartbeat", 0).to_u16
    frame_max = arguments.fetch("frame_max", 131_072).to_u32
    channel_max = arguments.fetch("channel_max", 1024).to_u16
    verify_mode = case arguments.fetch("verify", "").downcase
                  when "none" then OpenSSL::SSL::VerifyMode::NONE
                  else             OpenSSL::SSL::VerifyMode::PEER
                  end
    name = arguments.fetch("name", nil).try { |n| URI.decode_www_form(n) }
    tcp_nodelay = arguments.has_key?("tcp_nodelay")
    ka_args = arguments.fetch("tcp_keepalive", "60:10:3").split(":", 3)
    tcp_keepalive = {idle: ka_args[0].to_i, interval: ka_args[1].to_i, count: ka_args[2].to_i}
    self.new(host, port, vhost, user, password, tls, websocket,
      channel_max, frame_max, heartbeat, verify_mode, name,
      tcp_nodelay, tcp_keepalive)
  end

  getter host, port, vhost, user, tls, websocket

  def initialize(@host = AMQP_HOST, @port = AMQP_PORT, @vhost = AMQP_VHOST, @user = AMQP_USER, @password = AMQP_PASS,
                 @tls = AMQP_TLS, @websocket = AMQP_WS, @channel_max = 1024_u16, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 @verify_mode = OpenSSL::SSL::VerifyMode::PEER, @name : String? = File.basename(PROGRAM_NAME),
                 @tcp_nodelay = false, @tcp_keepalive = {idle: 60, interval: 10, count: 3})
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
    elsif @tls
      socket = connect_tls(connect_tcp)
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    else
      socket = connect_tcp
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    end
  rescue ex
    raise ex if ex.is_a?(Error)
    raise Error.new(ex.message, cause: ex)
  end

  private def connect_tcp
    socket = TCPSocket.new(@host, @port, connect_timeout: 15)
    socket.keepalive = true
    socket.tcp_nodelay = @tcp_nodelay
    socket.tcp_keepalive_idle = @tcp_keepalive[:idle]
    socket.tcp_keepalive_count = @tcp_keepalive[:count]
    socket.tcp_keepalive_interval = @tcp_keepalive[:interval]
    socket.sync = false
    socket.read_buffering = true
    socket.buffer_size = 16384
    socket
  end

  private def connect_tls(socket)
    socket.sync = true
    socket.read_buffering = false
    ctx = OpenSSL::SSL::Context::Client.new
    ctx.verify_mode = @verify_mode
    tls_socket = OpenSSL::SSL::Socket::Client.new(socket, ctx, sync_close: true, hostname: @host)
    tls_socket.sync = false
    tls_socket.read_buffering = true
    tls_socket.buffer_size = 16384
    tls_socket
  end

  private def connect_unix
    UNIXSocket.new(@host).tap do |socket|
      socket.sync = false
      socket.read_buffering = true
      socket.buffer_size = 16384
    end
  end

  # :nodoc:
  alias Frame = AMQ::Protocol::Frame
  # :nodoc:
  alias Arguments = AMQ::Protocol::Table
  # :nodoc:
  alias Properties = AMQ::Protocol::Properties
end
