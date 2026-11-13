require "./spec_helper"

private def fake_amqp_handshake(sock : TCPSocket) : Nil
  format = IO::ByteFormat::NetworkEndian
  sock.read_fully(Bytes.new(8))
  sock.write_bytes(AMQ::Protocol::Frame::Connection::Start.new, format)
  sock.flush
  AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # StartOk
  sock.write_bytes(AMQ::Protocol::Frame::Connection::Tune.new, format)
  sock.flush
  AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # TuneOk
  AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Open
  sock.write_bytes(AMQ::Protocol::Frame::Connection::OpenOk.new, format)
  sock.flush
end

private def fake_amqp_server_that_closes(done : ::Channel(Nil)) : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port

  spawn(name: "fake-amqp-server") do
    sock = server.accept
    fake_amqp_handshake(sock)
    sock.close
  ensure
    server.close
    done.send nil
  end

  port
end

# Accepts one connection, completes the handshake, then calls *handler*.
# The socket stays open until *handler* returns.
private def fake_amqp_server(&handler : TCPSocket ->) : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port

  spawn(name: "fake-amqp-server") do
    sock = server.accept
    fake_amqp_handshake(sock)
    handler.call(sock)
  ensure
    sock.try &.close
    server.close
  end

  port
end

private class ProxySwitch
  property? frozen = false
end

private def proxy_pipe(from : TCPSocket, to : TCPSocket, switch : ProxySwitch) : Nil
  spawn(name: "freezable-proxy-pipe") do
    buf = Bytes.new(4096)
    while (n = from.read(buf)) > 0
      while switch.frozen?
        sleep 10.milliseconds
      end
      to.write(buf[0, n])
    end
  rescue IO::Error
    # one side closed, the test is over
  end
end

# Forwards one connection to *upstream_port* until *switch* is frozen, then stops
# forwarding in both directions but keeps the sockets open, like a dead peer.
private def freezable_proxy(upstream_port : Int32, switch : ProxySwitch) : Int32
  server = TCPServer.new("127.0.0.1", 0)
  spawn(name: "freezable-proxy") do
    client = server.accept
    upstream = TCPSocket.new("127.0.0.1", upstream_port)
    proxy_pipe(client, upstream, switch)
    proxy_pipe(upstream, client, switch)
  ensure
    server.close
  end
  server.local_address.port
end

describe AMQP::Client::Connection do
  describe "network-level disconnect" do
    it "invokes on_disconnect with the exception when read_loop sees IO::Error" do
      done = ::Channel(Nil).new
      port = fake_amqp_server_that_closes(done)

      callback_received = ::Channel(Exception).new(1)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect do |ex|
        callback_received.send(ex)
      end
      done.receive

      select
      when ex = callback_received.receive
        ex.should be_a(IO::Error)
      when timeout(2.seconds)
        fail "on_disconnect was not invoked within 2s of peer disconnect"
      end

      conn.closed?.should be_true
    end

    it "does not invoke on_close on a network-level disconnect" do
      done = ::Channel(Nil).new
      port = fake_amqp_server_that_closes(done)

      on_close_received = ::Channel({UInt16, String}).new(1)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_close do |code, text|
        on_close_received.send({code, text})
      end
      done.receive

      30.times do
        break if conn.closed?
        sleep 50.milliseconds
      end
      conn.closed?.should be_true

      select
      when on_close_received.receive
        fail "on_close should not fire on network-level disconnect"
      when timeout(200.milliseconds)
        # expected
      end
    end

    it "logs at error when no on_disconnect callback is registered" do
      done = ::Channel(Nil).new
      port = fake_amqp_server_that_closes(done)

      backend = ::Log::MemoryBackend.new
      Log.builder.bind "amqp.client.connection", :debug, backend

      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      done.receive

      30.times do
        break if conn.closed?
        sleep 50.milliseconds
      end
      conn.closed?.should be_true

      entry = backend.entries.find(&.message.includes?("connection closed unexpectedly"))
      entry.should_not be_nil
      entry.try(&.severity).should eq ::Log::Severity::Error
    end
  end

  describe "heartbeats" do
    it "sends a heartbeat when no other frame was written" do
      got_heartbeat = ::Channel(Bool).new(1)
      release = ::Channel(Nil).new(1)
      port = fake_amqp_server do |sock|
        sock.read_timeout = 5.seconds
        AMQ::Protocol::Stream.new(sock).next_frame do |f|
          got_heartbeat.send f.is_a?(AMQ::Protocol::Frame::Heartbeat)
        end
        release.receive
      end

      conn = AMQP::Client.new(host: "127.0.0.1", port: port, heartbeat: 1_u16).connect
      begin
        select
        when heartbeat = got_heartbeat.receive
          heartbeat.should be_true
        when timeout(2.seconds)
          fail "The client did not send a heartbeat within 2s"
        end
      ensure
        conn.close(no_wait: true)
        release.send nil
      end
    end

    it "closes the connection when the server sends nothing for 2 x heartbeat" do
      release = ::Channel(Nil).new(1)
      port = fake_amqp_server { |_sock| release.receive }

      disconnected = ::Channel(Exception).new(1)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port, heartbeat: 1_u16).connect
      conn.on_disconnect { |ex| disconnected.send ex }
      begin
        select
        when ex = disconnected.receive
          ex.should be_a(IO::TimeoutError)
        when timeout(4.seconds)
          fail "on_disconnect was not invoked within 4s of a silent server"
        end
        conn.closed?.should be_true
      ensure
        release.send nil
      end
    end

    it "makes a publish blocked on a full send buffer raise when the server goes silent" do
      release = ::Channel(Nil).new(1)
      port = fake_amqp_server do |sock|
        format = IO::ByteFormat::NetworkEndian
        AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Channel.Open
        sock.write_bytes(AMQ::Protocol::Frame::Channel::OpenOk.new(1_u16), format)
        sock.flush
        release.receive # stop reading so the client's send buffer fills up
      end

      conn = AMQP::Client.new(host: "127.0.0.1", port: port, heartbeat: 1_u16).connect
      conn.on_disconnect { |_ex| }
      ch = conn.channel
      publish_error = ::Channel(Exception).new(1)
      spawn do
        body = Bytes.new(1_000)
        loop { ch.basic_publish(body, "", "q") }
      rescue ex
        publish_error.send ex
      end
      begin
        select
        when ex = publish_error.receive
          # write raises the flush error unwrapped
          ex.should be_a(IO::Error | AMQP::Client::Error)
        when timeout(5.seconds)
          fail "basic_publish was still blocked 5s after the server went silent"
        end
      ensure
        release.send nil
      end
    end

    it "makes a blocked publish raise when a WebSocket peer goes silent" do
      switch = ProxySwitch.new
      port = freezable_proxy(15_672, switch)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port, websocket: true, heartbeat: 1_u16).connect
      conn.on_disconnect { |_ex| }
      ch = conn.channel
      switch.frozen = true
      publish_error = ::Channel(Exception).new(1)
      spawn do
        body = Bytes.new(1_000)
        loop { ch.basic_publish(body, "", "q") }
      rescue ex
        publish_error.send ex
      end
      select
      when publish_error.receive
        30.times do
          break if ch.closed?
          sleep 50.milliseconds
        end
        ch.closed?.should be_true
      when timeout(5.seconds)
        fail "basic_publish was still blocked 5s after the WebSocket peer went silent"
      end
    ensure
      switch.try &.frozen = false
    end

    it "sends heartbeats while transaction publishes stay in the write buffer" do
      with_channel(heartbeat: 1_u16) do |ch|
        ch.tx_select
        q = ch.queue
        32.times do
          q.publish "x"
          sleep 250.milliseconds
        end
        ch.closed?.should be_false
        ch.tx_commit
      end
    end

    it "keeps an idle connection to the broker open" do
      with_channel(heartbeat: 1_u16) do |ch|
        sleep 3.seconds
        ch.closed?.should be_false
        q = ch.queue
        q.publish "hello"
        q.get(no_ack: true).try(&.body_io.to_s).should eq "hello"
      end
    end
  end
end
