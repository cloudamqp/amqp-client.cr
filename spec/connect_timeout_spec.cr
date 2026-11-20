require "./spec_helper"

# A peer that accepts the TCP connection and then never answers
private def silent_server(& : Int32 ->)
  server = TCPServer.new("127.0.0.1", 0)
  clients = [] of TCPSocket
  spawn(name: "silent-server") do
    while client = server.accept?
      clients << client
    end
  end
  yield server.local_address.port
ensure
  clients.try &.each &.close
  server.try &.close
end

# Runs the connect in its own fiber so that a hang fails the spec instead of blocking it
private def connect_result(client : AMQP::Client) : Exception?
  result = ::Channel(Exception?).new(1)
  spawn do
    client.connect.close
    result.send nil
  rescue ex
    result.send ex
  end
  select
  when ex = result.receive
    ex
  when timeout(5.seconds)
    fail "connect did not return"
  end
end

describe AMQP::Client do
  describe "#connect_timeout" do
    it "defaults to 60 seconds" do
      AMQP::Client.new.connect_timeout.should eq 60.seconds
    end

    it "bounds a TLS handshake that the peer never answers" do
      silent_server do |port|
        client = AMQP::Client.new(host: "127.0.0.1", port: port, tls: true)
        client.connect_timeout = 200.milliseconds
        connect_result(client).should be_a AMQP::Client::Error::NetworkError
      end
    end

    it "bounds a WebSocket upgrade that the peer never answers" do
      silent_server do |port|
        client = AMQP::Client.new(host: "127.0.0.1", port: port, websocket: true)
        client.connect_timeout = 200.milliseconds
        connect_result(client).should be_a AMQP::Client::Error::NetworkError
      end
    end
  end
end
