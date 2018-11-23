require "amq-protocol"
require "uri"
require "socket"
require "openssl"
require "./amqp-client/*"

# TODO: Write documentation for `Amqp::Client`
class AMQP::Client
  VERSION = "0.1.0"

  @io : TCPSocket | OpenSSL::SSL::Socket::Client | Nil

  def initalize(url : String)
    @uri = URI.parse(url)
    @uri.port ||= @uri.scheme == "amqps" ? 5671 : 5672
  end
    
  def connect
    socket = TCPSocket.new(@uri.host, @uri.port, connect_timeout: 5)
    socket.keepalive = true
    socket.tcp_nodelay = true
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.write_timeout = 15
    socket.recv_buffer_size = 131072

    if @uri.scheme == "amqps"
      @socket = OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: @uri.host)
    else
      @socket = socket
    end
    negotiate_connection(channel_max, heartbeat, auth_mechanism)
  end

  @channels = Hash(UInt16, Channel).new

  def channel(id : UInt16? = nil)
    if id && @channels.has_key? id
      return @channel[id]
    else
      1_u16.upto(UInt16::MAX) do |i|
       next if @channels.has_key? i
       id = i
       break
      end
    end
    Channel.new(@io, id)
  end

  def negotiate_connection(channel_max, heartbeat, auth_mechanism)
    @socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
    @socket.flush
    AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::Start) }

    props = {} of String => AMQP::Field
    user = URI.unescape(@uri.user || "guest")
    password = URI.unescape(@uri.password || "guest")
    response = "\u0000#{user}\u0000#{password}"
    write AMQP::Frame::Connection::StartOk.new(props, auth_mechanism, response, "")
    AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::Tune) }
    write AMQP::Frame::Connection::TuneOk.new(channel_max: channel_max,
                                              frame_max: 131072_u32, heartbeat: heartbeat)
    path = @uri.path || ""
    vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
    write AMQP::Frame::Connection::Open.new(vhost)
    AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::OpenOk) }
  end

  def open_channel
    write AMQP::Frame::Channel::Open.new(1_u16)
    AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Channel::OpenOk) }
  end

  def close(msg = "Connection closed")
    return if @socket.closed?
    write AMQP::Frame::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
  rescue Errno
    @log.info("Socket already closed, can't send close frame")
  end

  def write(frame)
    return if @socket.closed?
    @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
    @socket.flush
  end

  def closed?
    @socket.closed?
  end

end
