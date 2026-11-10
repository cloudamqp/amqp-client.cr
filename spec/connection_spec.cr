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

# Without a debug binding the sent line skips Log.with_context, so there is nothing to restore
private def with_debug_log(&)
  backend = ::Log::MemoryBackend.new
  Log.builder.bind "amqp.client.connection", :debug, backend
  yield
ensure
  Log.builder.unbind "amqp.client.connection", :debug, backend if backend
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

  describe "#name" do
    it "is nil when no name is given" do
      with_connection do |c|
        c.name.should be_nil
      end
    end

    it "is the connection name when one is given" do
      with_connection(name: "my-connection") do |c|
        c.name.should eq "my-connection"
      end
    end

    it "is the name URI parameter when one is given" do
      AMQP::Client.start("amqp://#{AMQP::Client::AMQP_HOST}?name=from-uri") do |c|
        c.name.should eq "from-uri"
      end
    end

    it "is nil when the name URI parameter is empty" do
      AMQP::Client.start("amqp://#{AMQP::Client::AMQP_HOST}?name=") do |c|
        c.name.should be_nil
      end
    end

    it "is included in the log lines of received and sent frames" do
      io = IO::Memory.new
      backend = ::Log::IOBackend.new(io, dispatcher: ::Log::DispatchMode::Sync)
      Log.builder.bind "amqp.client.connection", :debug, backend
      begin
        with_connection(name: "logged-connection") do |c|
          c.channel
        end
      ensure
        Log.builder.unbind "amqp.client.connection", :debug, backend
      end

      lines = io.to_s.lines
      %w[sent recv].each do |direction|
        line = lines.find(&.includes?(" #{direction} "))
        fail "no #{direction} line logged, got: #{lines}" if line.nil?
        line.should contain %(connection: "logged-connection")
      end
      io.to_s.each_line { |l| l.scan(%(connection: ")).size.should be <= 1 }
    end

    it "tags log lines with the address pair when no name is given" do
      io = IO::Memory.new
      backend = ::Log::IOBackend.new(io, dispatcher: ::Log::DispatchMode::Sync)
      Log.builder.bind "amqp.client.connection", :debug, backend
      begin
        with_connection do |c|
          c.log_tag.should match(/\A\S+:\d+ -> \S+:\d+\z/)
          c.channel
        end
      ensure
        Log.builder.unbind "amqp.client.connection", :debug, backend
      end

      line = io.to_s.lines.find(&.includes?(" sent "))
      fail "no sent line logged, got: #{io}" if line.nil?
      line.should match(/connection: "\S+:\d+ -> \S+:\d+"/)
    end

    it "has a different log tag for each connection without a name" do
      with_connection do |c1|
        with_connection do |c2|
          c1.log_tag.should_not eq c2.log_tag
        end
      end
    end

    it "uses a random log tag when the transport has no address pair" do
      with_ws_connection do |c|
        c.name.should be_nil
        c.log_tag.should match(/\A[0-9a-f]{8}\z/)
      end
    end

    it "restores the caller's log context after publish and close" do
      with_debug_log do
        Log.with_context(request: "r1") do
          with_channel(name: "restored") do |ch|
            ch.basic_publish("x", "", "nonexistent")
            Log.context.metadata.should eq Log::Metadata.build({request: "r1"})
          end
          Log.context.metadata.should eq Log::Metadata.build({request: "r1"})
        end
        with_channel do |ch|
          ch.basic_publish("x", "", "nonexistent")
          Log.context.metadata.empty?.should be_true
        end
        Log.context.metadata.empty?.should be_true
      end
    end

    it "restores a consumer's log context after it publishes on another connection" do
      with_debug_log do
        with_channel(name: "pub") do |pub_ch|
          with_channel(name: "sub") do |sub_ch|
            q = sub_ch.queue("")
            contexts = Channel({String?, String?}).new
            q.subscribe(no_ack: true) do |_|
              before = Log.context.metadata[:connection]?.try(&.as_s)
              pub_ch.basic_publish("x", "", "nonexistent")
              contexts.send({before, Log.context.metadata[:connection]?.try(&.as_s)})
            end
            q.publish "go"
            contexts.receive.should eq({"sub", "sub"})
          end
        end
      end
    end

    it "doesn't allocate on publish when debug logging is off" do
      # Clears bindings other specs left at debug; Log#level= would stick as an override
      Log.setup(:info)
      begin
        Log.with_context(request: "r1") do
          with_channel(name: "alloc") do |ch|
            body = Bytes.new(1)
            100.times { ch.basic_publish(body, "", "nonexistent") }
            GC.collect
            before = GC.stats.total_bytes
            n = 10_000
            n.times { ch.basic_publish(body, "", "nonexistent") }
            ((GC.stats.total_bytes - before) / n).should be < 8
          end
        end
      ensure
        Log.setup_from_env
      end
    end

    it "keeps the caller's log context in log lines" do
      io = IO::Memory.new
      backend = ::Log::IOBackend.new(io, dispatcher: ::Log::DispatchMode::Sync)
      Log.builder.bind "amqp.client.*", :debug, backend
      begin
        Log.with_context(request: "r1") do
          with_channel(name: "ctx") do |ch|
            ch.basic_publish("x", "", "nonexistent")
            ch.basic_cancel("unknown-tag")
          end
        end
      ensure
        Log.builder.unbind "amqp.client.*", :debug, backend
      end

      lines = io.to_s.lines
      %w[sent already\ cancelled].each do |text|
        line = lines.find(&.includes?(text))
        fail "no '#{text}' line logged, got: #{lines}" if line.nil?
        line.should contain %(request: "r1")
        line.should contain %(connection: "ctx")
      end
    end
  end
end
