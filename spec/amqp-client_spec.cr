require "./spec_helper"

describe AMQP::Client do
  it "can connect" do
    with_channel do |ch|
      ch.should_not be_nil
    end
  end

  it "can publish" do
    with_channel do |ch|
      q = ch.queue
      q.publish "hej"
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.body_io.to_s.should eq "hej" if msg
    end
  end

  it "can consume" do
    s = Channel(Nil).new
    with_channel do |ch|
      q = ch.queue
      q.subscribe do |msg|
        msg.should_not be_nil
        s.send nil
      end
      q.publish("hej!")
      s.receive
    end
  end

  it "can publish msg larger than frame_max" do
    with_channel do |ch|
      q = ch.queue
      str = "a" * 257 * 1024
      q.publish str
      msg = q.get
      msg.should_not be_nil
      msg.body_io.to_s.should eq str if msg
    end
  end

  it "raises ClosedException if trying to delete non empty queue" do
    with_channel do |ch|
      q = ch.queue
      q.publish ""
      expect_raises(AMQP::Client::Channel::ClosedException) do
        q.delete(if_empty: true)
      end
    end
  end

  it "can delete a queue" do
    with_channel do |ch|
      q = ch.queue("crystal-q1")
      q.publish ""
      deleted_q = q.delete
      deleted_q[:message_count].should eq 1
      expect_raises(AMQP::Client::Channel::ClosedException) do
        ch.queue_declare("crystal-q1", passive: true)
      end
    end
  end

  it "can purge a queue" do
    with_channel do |ch|
      q = ch.queue
      q.publish ""
      ok = q.purge
      ok[:message_count].should eq 1
    end
  end

  it "can declare and publish to an exchange" do
    with_channel do |ch|
      q = ch.queue("crystal-q2")
      x = ch.default_exchange
      x.publish "hej", q.name
      q.delete[:message_count].should eq 1
    end
  end

  it "can publish with confirm" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm("hej").should eq true
      ok = q.delete
      ok[:message_count].should eq 1
    end
  end

  it "can use blocks" do
    with_channel do |ch|
      ch.basic_publish_confirm("hej", "", "my-queue").should eq true
    end
  end

  it "can publish and consume properties" do
    with_channel do |ch|
      q = ch.queue
      props = AMQ::Protocol::Properties.new(content_type: "text/plain", delivery_mode: 1_u8)
      q.publish "hej", props: props
      if msg = q.get(no_ack: true)
        msg.properties.content_type.should eq props.content_type
        msg.properties.delivery_mode.should eq props.delivery_mode
      else
        msg.should_not be_nil
      end
    end
  end

  it "can get multiple messages" do
    with_channel do |ch|
      q = ch.queue
      props1 = AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"h" => "1"}))
      props2 = AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"h" => "2"} of String => AMQ::Protocol::Field))
      q.publish_confirm "1", props: props1
      q.publish_confirm "2", props: props2
      msg1 = q.get(no_ack: true)
      msg2 = q.get(no_ack: true)
      msg1.should_not be_nil
      msg1.body_io.to_s.should eq "1" if msg1
      msg1.properties.headers.should eq props1.headers if msg1
      msg2.should_not be_nil
      msg2.body_io.to_s.should eq "2" if msg2
      msg2.properties.headers.should eq props2.headers if msg2
    end
  end

  it "can send consumer flow" do
    with_channel do |ch|
      q = ch.queue
      msg = nil
      q.subscribe do |m|
        msg = m
      end
      ch.flow(false)
      q.publish("hej!")
      sleep 0.05
      msg.should be_nil
    end
  end
end
