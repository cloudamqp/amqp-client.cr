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

  def self.start(host = "localhost", port = 5672, vhost = "/", user = "guest", password = "guest", tls = false, heartbeat = 0_u16, frame_max = 131_072_u32, log_level = Logger::WARN, &blk : AMQP::Client::Connection -> Nil)
    conn = self.new(host, port, vhost, user, password, tls, heartbeat, frame_max, log_level).connect
    yield conn
  ensure
    conn.try &.close
  end

  def initialize(url : String, log_level = Logger::WARN)
    uri = URI.parse(url)
    @tls = uri.scheme == "amqps"
    @host = uri.host || "localhost"
    @port = uri.port || @tls ? 5671 : 5672
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
    @log = Logger.new(STDOUT, level: log_level)
  end

  def initialize(@host = "localhost", @port = 5672, @vhost = "/", @user = "guest", @password = "guest", @tls = false, @heartbeat = 0_u16, @frame_max = 131_072_u32, log_level = Logger::WARN)
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
    tune =
      if @tls
        io = OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: @host)
        negotiate_connection(io)
      else
        negotiate_connection(socket)
      end
    Connection.new(socket, @log, tune[:channel_max], tune[:frame_max])
  end

  private def negotiate_connection(io : TCPSocket | OpenSSL::SSL::Socket::Client, heartbeat = 0_u16)
    io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
    io.flush
    AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::Start) }

    props = {} of String => AMQ::Protocol::Field
    user = URI.unescape(@user)
    password = URI.unescape(@password)
    response = "\u0000#{user}\u0000#{password}"
    io.write_bytes AMQ::Protocol::Frame::Connection::StartOk.new(props, "PLAIN", response, ""), IO::ByteFormat::NetworkEndian
    io.flush
    tune = AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::Tune) }
    channel_max = tune.channel_max.zero? ? UInt16::MAX : tune.channel_max
    frame_max = tune.frame_max.zero? ? 131072_u32 : tune.frame_max
    io.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(channel_max: channel_max,
                                                                frame_max: frame_max, heartbeat: heartbeat), IO::ByteFormat::NetworkEndian
    io.write_bytes AMQ::Protocol::Frame::Connection::Open.new(@vhost), IO::ByteFormat::NetworkEndian
    io.flush
    AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::OpenOk) }

    { channel_max: channel_max, frame_max: frame_max }
  end
end
