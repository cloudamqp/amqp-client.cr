require "./spec_helper"

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

    it "raises on an unsupported mechanism" do
      expect_raises(AMQP::Client::Error, /Unsupported authentication mechanism/) do
        AMQP::Client.new(auth_mechanism: "SCRAM-SHA-256").connect
      end
    end
  end
end
