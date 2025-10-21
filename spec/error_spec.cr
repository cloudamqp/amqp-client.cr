require "./spec_helper"

describe "Error Handling" do
  describe "Network errors" do
    it "wraps DNS resolution failure" do
      expect_raises(AMQP::Client::Error::NetworkError, /invalid-hostname-that-does-not-exist/) do
        AMQP::Client.new(host: "invalid-hostname-that-does-not-exist.example").connect
      end
    end

    it "wraps connection refused" do
      expect_raises(AMQP::Client::Error::NetworkError, /Connection refused/) do
        AMQP::Client.new(port: 1).connect
      end
    end

    it "wraps TLS error on non-TLS port" do
      expect_raises(AMQP::Client::Error::NetworkError) do
        AMQP::Client.new(port: 5672, tls: true).connect
      end
    end

    it "preserves original exception via cause" do
      begin
        AMQP::Client.new(port: 1).connect
      rescue ex : AMQP::Client::Error::NetworkError
        ex.cause.should be_a(Socket::Error)
        ex.host.should eq("localhost")
        ex.port.should eq(1)
      end
    end

    it "includes host and port in error message" do
      begin
        AMQP::Client.new(host: "127.0.0.1", port: 1).connect
      rescue ex : AMQP::Client::Error::NetworkError
        if msg = ex.message
          msg.should contain("127.0.0.1:1")
        else
          fail "Expected error message to be present"
        end
      end
    end
  end

  describe "Exception hierarchy" do
    it "NetworkError is an AMQP::Client::Error" do
      begin
        AMQP::Client.new(port: 1).connect
      rescue ex : AMQP::Client::Error
        ex.should be_a(AMQP::Client::Error::NetworkError)
      end
    end

    it "can catch all AMQP errors generically" do
      caught = false
      begin
        AMQP::Client.new(port: 1).connect
      rescue ex : AMQP::Client::Error
        caught = true
        ex.cause.should_not be_nil
      end
      caught.should be_true
    end
  end

  describe "Existing error tests remain compatible" do
    it "IO::Errors are raised as AMQP::Client::Error::NetworkError" do
      expect_raises(AMQP::Client::Error::NetworkError) do
        AMQP::Client.new(port: 1).connect
      end
    end

    it "TLS exceptions are raised as AMQP::Client::Error::NetworkError" do
      expect_raises(AMQP::Client::Error::NetworkError) do
        AMQP::Client.new(port: 5672, tls: true).connect
      end
    end
  end
end
