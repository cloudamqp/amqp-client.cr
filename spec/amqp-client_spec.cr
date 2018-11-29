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
    q = ch.queue("myqueue")
    q.publish "hej"
    msg = q.get(no_ack: true)
    msg.should_not be_nil
    msg.body_io.to_s.should eq "hej" if msg
    c.close
  end

  it "can consume" do
    s = Channel(Nil).new
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    q = ch.queue("myqueue")
    q.subscribe do |msg|
      msg.should_not be_nil
      s.send nil
    end
    q.publish("hej!")
    s.receive
    c.close
  end

  it "can publish msg larger than frame_max" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    q = ch.queue
    str = "a" * 257 * 1024
    q.publish str
    msg = q.get
    msg.should_not be_nil
    msg.body_io.to_s.should eq str if msg
    c.close
  end

  it "raises ClosedException if trying to delete non empty queue" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    q = ch.queue
    q.publish ""
    expect_raises(AMQP::Client::Channel::ClosedException) do
      q.delete(if_empty: true)
    end
    c.close
  end

  it "can delete a queue" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    q = ch.queue("q1")
    q.publish ""
    deleted_q = q.delete
    deleted_q[:message_count].should eq 1
    expect_raises(AMQP::Client::Channel::ClosedException) do
      ch.queue_declare("q1", passive: true)
    end
    c.close
  end

  it "can delete a purge" do
    c = AMQP::Client.new("amqp://guest:guest@localhost").connect
    ch = c.channel
    q = ch.queue("q1")
    q.publish ""
    ok = q.purge
    ok[:message_count].should eq 1
    c.close
  end
end
