require "./spec_helper"

describe "AMQP::Client#connect(address)" do
  it "dials the given address without resolving host" do
    client = AMQP::Client.new(host: "does-not-exist.invalid")
    conn = client.connect(Socket::IPAddress.new("127.0.0.1", 5672))
    conn.closed?.should be_false
  ensure
    conn.try &.close
  end

  it "uses host for TLS SNI" do
    server = TCPServer.new("127.0.0.1", 0)
    client_hello = Channel(String).new(1)
    spawn do
      sock = server.accept
      buf = Bytes.new(4096)
      client_hello.send String.new(buf[0, sock.read(buf)])
      sock.close
    end
    client = AMQP::Client.new(host: "example.test", tls: true)
    expect_raises(Exception) do
      client.connect(server.local_address)
    end
    client_hello.receive.should contain("example.test")
  ensure
    server.try &.close
  end

  it "wraps network errors with the dialed address" do
    client = AMQP::Client.new(host: "example.test", port: 5672)
    ex = expect_raises(AMQP::Client::Error::NetworkError) do
      client.connect(Socket::IPAddress.new("127.0.0.1", 1))
    end
    ex.host.should eq "127.0.0.1"
    ex.port.should eq 1
  end

  it "raises ArgumentError for websocket clients" do
    client = AMQP::Client.new(websocket: true)
    expect_raises(ArgumentError) do
      client.connect(Socket::IPAddress.new("127.0.0.1", 15_672))
    end
  end

  it "raises ArgumentError for unix socket clients" do
    client = AMQP::Client.new(host: "/tmp/amqp.sock")
    expect_raises(ArgumentError) do
      client.connect(Socket::IPAddress.new("127.0.0.1", 5672))
    end
  end
end
