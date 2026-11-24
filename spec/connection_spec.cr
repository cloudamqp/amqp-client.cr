require "./spec_helper"

private def fake_amqp_server(&after_open : TCPSocket -> Nil) : Int32
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
    after_open.call(sock)
  ensure
    sock.try &.close
    server.close
  end

  port
end

private def fake_amqp_server_that_closes(done : ::Channel(Nil)) : Int32
  fake_amqp_server do |sock|
    sock.close
    done.send nil
  end
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

    it "invokes on_disconnect when a flush fails, though reads still work" do
      stop = ::Channel(Nil).new
      port = fake_amqp_server { |_sock| stop.receive? }

      callback_received = ::Channel(Exception).new(1)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect do |ex|
        callback_received.send(ex)
      end
      conn.@io.as(TCPSocket).close_write
      conn.write_coalesced AMQ::Protocol::Frame::Heartbeat.new

      select
      when ex = callback_received.receive
        ex.should be_a(IO::Error)
      when timeout(2.seconds)
        fail "on_disconnect was not invoked within 2s of a failed flush"
      end
      conn.closed?.should be_true
    ensure
      stop.try &.close
    end

    it "raises the write error from a call that waits for a reply" do
      stop = ::Channel(Nil).new
      port = fake_amqp_server { |_sock| stop.receive? }

      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect { }
      conn.@io.as(TCPSocket).close_write
      expect_raises(IO::Error) { conn.channel }
    ensure
      stop.try &.close
    end

    it "replies CloseOk when the server closes the connection" do
      reply = ::Channel(AMQ::Protocol::Frame).new(1)
      port = fake_amqp_server do |sock|
        sock.write_bytes(AMQ::Protocol::Frame::Connection::Close.new(320_u16, "CONNECTION_FORCED", 0_u16, 0_u16), IO::ByteFormat::NetworkEndian)
        sock.flush
        AMQ::Protocol::Stream.new(sock).next_frame { |f| reply.send f }
      end

      AMQP::Client.new(host: "127.0.0.1", port: port).connect
      select
      when f = reply.receive
        f.should be_a(AMQ::Protocol::Frame::Connection::CloseOk)
      when timeout(2.seconds)
        fail "no CloseOk within 2s of the server's Close"
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

    it "raises on a write after the connection is lost" do
      done = ::Channel(Nil).new
      port = fake_amqp_server_that_closes(done)

      disconnected = ::Channel(Nil).new(1)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect { disconnected.send nil }
      done.receive
      select
      when disconnected.receive
      when timeout(2.seconds)
        fail "on_disconnect was not invoked within 2s of peer disconnect"
      end

      expect_raises(AMQP::Client::Connection::ClosedException, "Connection lost") do
        conn.write AMQ::Protocol::Frame::Heartbeat.new
      end
    end

    it "raises from a call waiting for its reply when the connection is lost" do
      port = fake_amqp_server do |sock|
        AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Channel.Open
        sock.write_bytes(AMQ::Protocol::Frame::Channel::OpenOk.new(1_u16), IO::ByteFormat::NetworkEndian)
        sock.flush
        AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Queue.Declare
      end

      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect { }
      ch = conn.channel

      expect_raises(AMQP::Client::Connection::ClosedException, "Connection lost") do
        ch.queue_declare("q")
      end
    end

    it "raises from update_secret when the connection is lost" do
      port = fake_amqp_server do |sock|
        AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Connection.UpdateSecret
      end

      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      conn.on_disconnect { }

      expect_raises(AMQP::Client::Connection::ClosedException, "Connection lost") do
        conn.update_secret("secret", "reason")
      end
    end
  end

  describe "closed by the client" do
    it "raises from calls on the connection's channels" do
      with_connection do |c|
        ch = c.channel
        c.close
        expect_raises(AMQP::Client::Connection::ClosedException, "Connection closed by client") do
          ch.basic_publish "", "", "q"
        end
      end
    end

    it "raises from calls on the channel" do
      with_connection do |c|
        ch = c.channel
        ch.close
        expect_raises(AMQP::Client::Channel::ClosedException, "Channel closed by client") do
          ch.basic_publish "", "", "q"
        end
      end
    end

    it "raises from a call waiting for its reply" do
      with_connection do |c|
        ch = c.channel
        result = ::Channel(Exception?).new(1)
        spawn do
          ch.queue_declare("", durable: false, exclusive: true)
          result.send nil
        rescue ex
          result.send ex
        end
        Fiber.yield
        c.close(no_wait: true)
        select
        when ex = result.receive
          ex.should be_a AMQP::Client::Connection::ClosedException
        when timeout(1.second)
          fail "queue_declare didn't return"
        end
      end
    end

    it "doesn't call on_disconnect after close without waiting" do
      with_connection do |c|
        disconnected = ::Channel(Exception).new(1)
        c.on_disconnect { |ex| disconnected.send ex }
        c.close(no_wait: true)
        select
        when ex = disconnected.receive
          fail "on_disconnect called: #{ex.message}"
        when timeout(200.milliseconds)
        end
      end
    end

    it "keeps the socket open for a close in progress when called again" do
      with_connection do |c|
        disconnected = ::Channel(Exception).new(1)
        c.on_disconnect { |ex| disconnected.send ex }
        first = ::Channel(Nil).new(1)
        spawn do
          c.close
          first.send nil
        end
        Fiber.yield
        c.close
        within(5.seconds, "the first close") { first.receive }
        select
        when ex = disconnected.receive
          fail "on_disconnect called: #{ex.message}"
        when timeout(100.milliseconds)
        end
      end
    end

    it "raises from wait_for_confirms with publishes still unconfirmed" do
      with_connection do |c|
        ch = c.channel
        ch.confirm_select
        q = ch.queue("", durable: false, exclusive: true)
        100.times { q.publish "" }
        c.close(no_wait: true)
        expect_raises(AMQP::Client::Connection::ClosedException) { ch.wait_for_confirms }
      end
    end

    it "raises from a wait_for_confirms that is waiting" do
      port = fake_amqp_server do |sock|
        loop do
          AMQ::Protocol::Stream.new(sock).next_frame do |f|
            reply = case f
                    when AMQ::Protocol::Frame::Channel::Open   then AMQ::Protocol::Frame::Channel::OpenOk.new(f.channel)
                    when AMQ::Protocol::Frame::Confirm::Select then AMQ::Protocol::Frame::Confirm::SelectOk.new(f.channel)
                    end
            if reply
              sock.write_bytes reply, IO::ByteFormat::NetworkEndian
              sock.flush
            end
          end
        end
      rescue IO::Error
      end

      conn = AMQP::Client.new(host: "127.0.0.1", port: port).connect
      ch = conn.channel
      ch.confirm_select
      ch.basic_publish "", "", "q"
      result = ::Channel(Exception?).new(1)
      spawn do
        ch.wait_for_confirms
        result.send nil
      rescue ex
        result.send ex
      end
      Fiber.yield
      conn.close(no_wait: true)
      within(5.seconds, "wait_for_confirms") do
        result.receive.should be_a AMQP::Client::Connection::ClosedException
      end
    end

    it "raises from transaction what the block raised" do
      with_channel do |ch|
        expect_raises(Exception, "boom") do
          ch.transaction do
            ch.close
            raise "boom"
          end
        end
      end
    end
  end

  it "raises the server's reason from a channel close that crossed the server's" do
    with_channel do |ch|
      ch.basic_publish "", "no-such-exchange"
      expect_raises(AMQP::Client::Channel::ClosedException, /NOT_FOUND/) { ch.close }
    end
  end
end
