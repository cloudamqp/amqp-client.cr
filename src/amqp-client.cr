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
                 log_level = Logger::WARN, &blk : AMQP::Client::Connection -> Nil)
    conn = self.new(host, port, vhost, user, password, tls, channel_max, frame_max, heartbeat, log_level).connect
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
    @user = uri.password || "guest"
    @password = uri.password || "guest"
    arguments = uri.query.try(&.split("&").map(&.split("=")).to_h) || Hash(String, String).new
    @heartbeat = arguments.fetch("heartbeat", 0_u16).to_u16
    @frame_max = arguments.fetch("frame_max", 131_072_u32).to_u32
    @channel_max = arguments.fetch("channel_max", UInt16::MAX).to_u16
    @log = Logger.new(STDOUT, level: log_level)
  end

  def initialize(@host = "localhost", @port = 5672, @vhost = "/", @user = "guest", @password = "guest",
                 @tls = false, @channel_max = UInt16::MAX, @frame_max = 131_072_u32, @heartbeat = 0_u16,
                 log_level = Logger::WARN)
    @log = Logger.new(STDOUT, level: log_level)
  end
    
  def connect : Connection
    socket = TCPSocket.new(@host, @port, connect_timeout: 5)
    socket.sync = false
    socket.keepalive = true
    socket.tcp_nodelay = true
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.write_timeout = 15
    if @tls
      tls_socket = OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: @host)
      Connection.start(tls_socket, @log, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat)
    else
      Connection.start(socket, @log, @user, @password, @vhost, @channel_max, @frame_max, @heartbeat)
    end
  end

  alias Frame = AMQ::Protocol::Frame
  alias Arguments = Hash(String, AMQ::Protocol::Field)
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
