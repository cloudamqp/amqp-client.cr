require "./spec_helper"

private def fake_amqp_server_that_closes(done : ::Channel(Nil)) : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  format = IO::ByteFormat::NetworkEndian

  spawn(name: "fake-amqp-server") do
    sock = server.accept
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
    sock.close
  ensure
    server.close
    done.send nil
  end

  port
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
end
