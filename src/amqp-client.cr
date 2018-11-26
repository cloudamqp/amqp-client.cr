require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "./amqp-client/*"
require "logger"

# TODO: Write documentation
class AMQP::Client
  VERSION = "0.1.0"

  @io : TCPSocket | OpenSSL::SSL::Socket::Client | Nil

  def initialize(url : String)
    @uri = URI.parse(url)
    @uri.port ||= @uri.scheme == "amqps" ? 5671 : 5672
    @log = Logger.new(STDOUT)
  end
    
  def connect
    socket = TCPSocket.new(@uri.host || "localhost", @uri.port || 5672,
                           connect_timeout: 5)
    socket.keepalive = true
    socket.tcp_nodelay = true
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.write_timeout = 15
    socket.recv_buffer_size = 131072

    if @uri.scheme == "amqps"
      @io = OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: @uri.host)
    else
      @io = socket
    end
    negotiate_connection
  end

  @channels = Hash(UInt16, Channel).new

  def channel(id : UInt16? = nil)
    if id && @channels.has_key? id
      return @channels[id]
    else
      1_u16.upto(UInt16::MAX) do |i|
       next if @channels.has_key? i
       id = i
       break
      end
    end
    Channel.new(self, id.not_nil!)
  end

  def negotiate_connection(heartbeat = 0_u16)
    @io.not_nil!.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
    @io.not_nil!.flush
    read { |f| f.as(AMQ::Protocol::Frame::Connection::Start) }

    props = {} of String => AMQ::Protocol::Field
    user = URI.unescape(@uri.user || "guest")
    password = URI.unescape(@uri.password || "guest")
    response = "\u0000#{user}\u0000#{password}"
    write AMQ::Protocol::Frame::Connection::StartOk.new(props, "PLAIN", response, "")
    read { |f| f.as(AMQ::Protocol::Frame::Connection::Tune) }
    write AMQ::Protocol::Frame::Connection::TuneOk.new(channel_max: 0_u16,
                                              frame_max: 131072_u32, heartbeat: heartbeat)
    path = @uri.path || ""
    vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
    write AMQ::Protocol::Frame::Connection::Open.new(vhost)
    read { |f| f.as(AMQ::Protocol::Frame::Connection::OpenOk) }
  end

  def open_channel
    write AMQ::Protocol::Frame::Channel::Open.new(1_u16)
    read { |f| f.as(AMQ::Protocol::Frame::Channel::OpenOk) }
  end

  def close(msg = "Connection closed")
    write AMQ::Protocol::Frame::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
  rescue Errno
    @log.info("Socket already closed, can't send close frame")
  end

  def write(frame)
    @io.not_nil!.write_bytes frame, ::IO::ByteFormat::NetworkEndian
    @io.not_nil!.flush
  end

  def read(&blk : AMQ::Protocol::Frame -> _)
    AMQ::Protocol::Frame.from_io(@io.not_nil!, &blk)
  end

  def closed?
    @io.closed?
  end
end
