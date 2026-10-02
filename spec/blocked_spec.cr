require "./spec_helper"

# A minimal AMQP server that accepts one connection and opens channels,
# so specs can send connection.blocked/unblocked to the client
private class FakeServer
  getter port : Int32

  def initialize
    @server = TCPServer.new("localhost", 0)
    @port = @server.local_address.port
    @frames = ::Channel(AMQ::Protocol::Frame).new(16)
    @socket = ::Channel(TCPSocket).new(1)
    spawn accept
  end

  def socket : TCPSocket
    @socket.receive
  end

  def send(frame : AMQ::Protocol::Frame) : Nil
    s = socket
    s.write_bytes frame, IO::ByteFormat::NetworkEndian
    s.flush
    @socket.send s
  end

  # Next frame the client sent after the handshake, or nil on timeout
  def next_frame(timeout : Time::Span) : AMQ::Protocol::Frame?
    select
    when f = @frames.receive? then f
    when timeout(timeout) then nil
    end
  end

  def disconnect : Nil
    socket.close
  end

  def close : Nil
    @server.close
  end

  private def accept
    socket = @server.accept? || return
    socket.read_fully(Bytes.new(8)) # protocol header
    socket.write_bytes AMQ::Protocol::Frame::Connection::Start.new, IO::ByteFormat::NetworkEndian
    socket.flush
    AMQ::Protocol::Frame.from_io(socket, &.as(AMQ::Protocol::Frame::Connection::StartOk))
    socket.write_bytes AMQ::Protocol::Frame::Connection::Tune.new, IO::ByteFormat::NetworkEndian
    socket.flush
    AMQ::Protocol::Frame.from_io(socket, &.as(AMQ::Protocol::Frame::Connection::TuneOk))
    AMQ::Protocol::Frame.from_io(socket, &.as(AMQ::Protocol::Frame::Connection::Open))
    socket.write_bytes AMQ::Protocol::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
    socket.flush
    @socket.send socket
    loop do
      frame = AMQ::Protocol::Frame.from_io(socket) do |f|
        if body = f.as?(AMQ::Protocol::Frame::Body)
          body.body.skip(body.body_size)
        end
        f
      end
      if open = frame.as?(AMQ::Protocol::Frame::Channel::Open)
        send AMQ::Protocol::Frame::Channel::OpenOk.new(open.channel)
      else
        @frames.send frame
      end
    end
  rescue IO::Error
  ensure
    @frames.close
  end
end

describe "connection.blocked" do
  it "basic_publish waits until the connection is unblocked" do
    server = FakeServer.new
    conn = AMQP::Client.new(port: server.port).connect
    ch = conn.channel
    server.send AMQ::Protocol::Frame::Connection::Blocked.new("low on memory")
    until conn.blocked?
      sleep 1.millisecond
    end

    published = ::Channel(Nil).new(1)
    spawn do
      ch.basic_publish "msg", "amq.direct", "rk"
      published.send nil
    end
    server.next_frame(100.milliseconds).should be_nil

    server.send AMQ::Protocol::Frame::Connection::Unblocked.new
    server.next_frame(1.second).should be_a(AMQ::Protocol::Frame::Basic::Publish)
    select
    when published.receive
    when timeout(1.second)
      fail "basic_publish did not return after unblocked"
    end
  ensure
    conn.try &.close(no_wait: true)
    server.try &.close
  end

  it "basic_publish stops waiting if the connection closes while blocked" do
    server = FakeServer.new
    conn = AMQP::Client.new(port: server.port).connect
    ch = conn.channel
    server.send AMQ::Protocol::Frame::Connection::Blocked.new("low on memory")
    until conn.blocked?
      sleep 1.millisecond
    end

    result = ::Channel(Exception?).new(1)
    spawn do
      ch.basic_publish "msg", "amq.direct", "rk"
      result.send nil
    rescue ex
      result.send ex
    end
    sleep 50.milliseconds
    select
    when result.receive
      fail "basic_publish returned while blocked"
    else
    end
    server.disconnect

    # Like any publish after a transport failure, it's dropped rather than
    # raising (disconnects are reported through on_disconnect), it just
    # mustn't keep waiting
    select
    when ex = result.receive
      ex.should be_nil
    when timeout(1.second)
      fail "basic_publish still waiting after the connection closed"
    end
    conn.closed?.should be_true
  ensure
    server.try &.close
  end

  it "basic_publish doesn't wait when not blocked" do
    server = FakeServer.new
    conn = AMQP::Client.new(port: server.port).connect
    ch = conn.channel
    ch.basic_publish "msg", "amq.direct", "rk"
    server.next_frame(1.second).should be_a(AMQ::Protocol::Frame::Basic::Publish)
  ensure
    conn.try &.close(no_wait: true)
    server.try &.close
  end
end
