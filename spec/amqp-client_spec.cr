require "./spec_helper"

describe AMQP::Client do
  it "can connect" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    ch.should_not be_nil
    c.close
  end

  it "can publish" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    ch.queue("myqueue")
    ch.publish(IO::Memory.new("hej"), "", "myqueue")
    msg = ch.get("myqueue", no_ack: true)
    msg.should_not be_nil
    msg.body_io.to_s.should eq "hej" if msg
    c.close
  end

  it "can consume" do
    s = Channel(Nil).new
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    ch.queue("myqueue")
    ch.consume("myqueue", true, false) do |msg|
      msg.should_not be_nil
      s.send nil
    end
    ch.publish("hej!", "", "myqueue")
    s.receive
    c.close
  end
end
