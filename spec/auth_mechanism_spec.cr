require "./spec_helper"

# Fake server that completes the handshake and reports the StartOk frame it received
private def fake_amqp_server_capturing_start_ok(start_ok : ::Channel(AMQ::Protocol::Frame::Connection::StartOk)) : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  format = IO::ByteFormat::NetworkEndian

  spawn(name: "fake-amqp-server") do
    sock = server.accept
    sock.read_fully(Bytes.new(8))
    sock.write_bytes(AMQ::Protocol::Frame::Connection::Start.new, format)
    sock.flush
    AMQ::Protocol::Stream.new(sock).next_frame do |f|
      start_ok.send f.as(AMQ::Protocol::Frame::Connection::StartOk)
    end
    sock.write_bytes(AMQ::Protocol::Frame::Connection::Tune.new, format)
    sock.flush
    AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # TuneOk
    AMQ::Protocol::Stream.new(sock).next_frame { |_f| } # Open
    sock.write_bytes(AMQ::Protocol::Frame::Connection::OpenOk.new, format)
    sock.flush
    sock.close
  ensure
    server.close
  end

  port
end

describe AMQP::Client do
  describe "auth_mechanism" do
    it "connects with the default PLAIN mechanism" do
      with_connection do |conn|
        conn.channel.should_not be_nil
      end
    end

    it "connects when PLAIN is requested explicitly" do
      with_connection(auth_mechanism: "PLAIN") do |conn|
        conn.channel.should_not be_nil
      end
    end

    it "parses auth_mechanism from the URI query" do
      client = AMQP::Client.new("amqp://guest:guest@localhost/?auth_mechanism=EXTERNAL")
      client.auth_mechanism.should eq "EXTERNAL"
    end

    it "defaults auth_mechanism to PLAIN when absent from the URI" do
      client = AMQP::Client.new("amqp://guest:guest@localhost/")
      client.auth_mechanism.should eq "PLAIN"
    end

    it "normalizes a lower-case mechanism to its canonical form" do
      client = AMQP::Client.new("amqp://guest:guest@localhost/?auth_mechanism=external")
      client.auth_mechanism.should eq "EXTERNAL"
    end

    it "normalizes a mechanism assigned after construction" do
      client = AMQP::Client.new("amqp://guest:guest@localhost/")
      client.auth_mechanism = "plain"
      client.auth_mechanism.should eq "PLAIN"
    end

    it "connects when a lower-case mechanism is requested" do
      with_connection(auth_mechanism: "plain") do |conn|
        conn.channel.should_not be_nil
      end
    end

    it "sends EXTERNAL with an empty response in StartOk" do
      start_ok = ::Channel(AMQ::Protocol::Frame::Connection::StartOk).new(1)
      port = fake_amqp_server_capturing_start_ok(start_ok)
      conn = AMQP::Client.new("amqp://guest:guest@127.0.0.1:#{port}/?auth_mechanism=external").connect
      frame = start_ok.receive
      frame.mechanism.should eq "EXTERNAL"
      frame.response.should eq ""
    ensure
      conn.try &.close(no_wait: true) rescue nil
    end

    it "sends PLAIN with the user and password in StartOk" do
      start_ok = ::Channel(AMQ::Protocol::Frame::Connection::StartOk).new(1)
      port = fake_amqp_server_capturing_start_ok(start_ok)
      conn = AMQP::Client.new(host: "127.0.0.1", port: port, user: "u", password: "p").connect
      frame = start_ok.receive
      frame.mechanism.should eq "PLAIN"
      frame.response.should eq "\u0000u\u0000p"
    ensure
      conn.try &.close(no_wait: true) rescue nil
    end

    it "raises on an unsupported mechanism" do
      expect_raises(AMQP::Client::Error, /Unsupported authentication mechanism/) do
        AMQP::Client.new(auth_mechanism: "SCRAM-SHA-256").connect
      end
    end
  end
end
