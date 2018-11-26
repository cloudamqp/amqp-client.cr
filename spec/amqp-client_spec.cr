require "./spec_helper"

describe AMQP::Client do
  it "can connect" do
    c = AMQP::Client.new("amqp://guest:guest@localhost")
    c.connect
    ch = c.channel
    ch.should_not be_nil
    c.close
  end
end
