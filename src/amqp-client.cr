require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "logger"
require "./amqp-client/*"

class AMQP::Client
  def self.start(url : String, log_level = Logger::WARN, &blk : AMQP::Client::Connection -> Nil)
    conn = self.new(url).connect
    yield conn
  ensure
    conn.try &.close
  end

  def self.start(host = "localhost", port = 5672, vhost = "/",
                 user = "guest", password = "guest", tls = false,
                 channel_max = UInt16::MAX, frame_max = 131_072_u32, heartbeat = 0_u16,
                 verify_mode = OpenSSL::SSL::VerifyMode::PEER,
                 log_level = Logger::WARN, &blk : AMQP::Client::Connection -> Nil)
    conn = self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, verify_mode, log_level).connect
    yield conn
  ensure
    conn.try &.close
  end

  def initialize(url : String, log_level = Logger::WARN)
    uri = URI.parse(url)
    @tls = uri.scheme == "amqps"
    @host = uri.host || "localhost"
    @port = uri.port || (@tls ? 5671 : 5672)
    @vhost = if uri.path.nil? || uri.path.not_nil!.empty?
               "/"
             else
               URI.unescape(uri.path.not_nil![1..-1])
             end
    @user = uri.user || "guest"
    @password = uri.password || "guest"
    arguments = uri.query.try(&.split("&").map(&.split("=")).to_h) || Hash(String, String).new
    @heartbeat = arguments.fetch("heartbeat", 0_u16).to_u16
    @frame_max = arguments.fetch("frame_max", 131_072_u32).to_u32
    @channel_max = arguments.fetch("channel_max", UInt16::MAX).to_u16
    @verify_mode = case arguments.fetch("verify", "").downcase
                   when "none" then OpenSSL::SSL::VerifyMode::NONE
                   else             OpenSSL::SSL::VerifyMode::PEER
                   end
    @log = Logger.new(STDOUT, level: log_level)
  end


  def initialize(@host = "localhost", @port = 5672, @vhost = "/", @user = "guest", @password = "guest",
                 @tls = false, @channel_max = UInt16::MAX, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 @verify_mode = OpenSSL::SSL::VerifyMode::PEER, log_level = Logger::WARN)
    @log = Logger.new(STDOUT, level: log_level)
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

  private def connect_tcp
    socket = TCPSocket.new(@host, @port, connect_timeout: 5)
    socket.keepalive = true
    socket.tcp_nodelay = true
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
