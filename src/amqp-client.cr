require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "./amqp-client/*"
require "logger"

# TODO: Write documentation
class AMQP::Client
  VERSION = "0.1.0"

  def initialize(url : String)
    @uri = URI.parse(url)
    @uri.port ||= @uri.scheme == "amqps" ? 5671 : 5672
    @log = Logger.new(STDOUT)
  end
    
  def connect : Connection
    socket = TCPSocket.new(@uri.host || "localhost", @uri.port || 5672, connect_timeout: 5)
    socket.sync = false
    socket.keepalive = true
    socket.tcp_nodelay = true
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.write_timeout = 15
    socket.recv_buffer_size = 131072

    channel_max =
      if @uri.scheme == "amqps"
        io = OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: @uri.host)
        negotiate_connection(io)
      else
        negotiate_connection(socket)
      end
    Connection.new(socket, @log, channel_max)
  end

  private def negotiate_connection(io : TCPSocket | OpenSSL::SSL::Socket::Client, heartbeat = 0_u16) : UInt16
    io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
    io.flush
    AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::Start) }

    props = {} of String => AMQ::Protocol::Field
    user = URI.unescape(@uri.user || "guest")
    password = URI.unescape(@uri.password || "guest")
    response = "\u0000#{user}\u0000#{password}"
    io.write_bytes AMQ::Protocol::Frame::Connection::StartOk.new(props, "PLAIN", response, ""), IO::ByteFormat::NetworkEndian
    io.flush
    tune = AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::Tune) }
    io.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(channel_max: 1024_u16,
                                                                frame_max: 131072_u32, heartbeat: heartbeat), IO::ByteFormat::NetworkEndian
    path = @uri.path || ""
    vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
    io.write_bytes AMQ::Protocol::Frame::Connection::Open.new(vhost), IO::ByteFormat::NetworkEndian
    io.flush
    AMQ::Protocol::Frame.from_io(io) { |f| f.as(AMQ::Protocol::Frame::Connection::OpenOk) }

    UInt16::MAX
  end
end
