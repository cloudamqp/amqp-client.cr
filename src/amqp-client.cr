require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "log"
require "./amqp-client/*"

class AMQP::Client
  LOG = ::Log.for(self)

  def self.start(url : String | URI, &blk : AMQP::Client::Connection -> _)
    conn = self.new(url).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.start(host = "localhost", port = 5672, vhost = "/",
                 user = "guest", password = "guest", tls = false,
                 channel_max = 1024_u16, frame_max = 131_072_u32, heartbeat = 0_u16,
                 verify_mode = OpenSSL::SSL::VerifyMode::PEER, name = nil, &blk : AMQP::Client::Connection -> _)
    conn = self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, verify_mode, name).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.new(url : String)
    uri = URI.parse(url)
    self.new(uri)
  end

  def self.new(uri : URI)
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
    self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, verify_mode, name)
  end

  def initialize(@host = "localhost", @port = 5672, @vhost = "/", @user = "guest", @password = "guest",
                 @tls = false, @channel_max = 1024_u16, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 @verify_mode = OpenSSL::SSL::VerifyMode::PEER, @name : String? = nil)
  end

  def connect : Connection
    if @host.starts_with? '/'
      socket = connect_unix
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    elsif @tls
      socket = connect_tls(connect_tcp)
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    else
      socket = connect_tcp
      Connection.start(socket, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat, @name)
    end
  end

  private def connect_tcp
    socket = TCPSocket.new(@host, @port, connect_timeout: 15)
    socket.keepalive = true
    socket.tcp_nodelay = false
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.sync = false
    socket.read_buffering = true
    socket.buffer_size = 16384
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
    tls_socket.buffer_size = 16384
    tls_socket
  end

  private def connect_unix
    UNIXSocket.new(@host).tap do |socket|
      socket.sync = false
      socket.read_buffering = true
      socket.buffer_size = 16384
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
