require "./spec_helper"

describe AMQP::Client do
  it "should connect" do
    with_channel do |ch|
      ch.should_not be_nil
    end
  end

  it "should connect to localhost when URI host is empty" do
    AMQP::Client.start("amqp:///%2f") do |c|
      c.channel.should_not be_nil
    end
  end

  it "should connect to localhost when URI path is empty" do
    AMQP::Client.start("amqp://localhost/") do |c|
      c.channel.should_not be_nil
    end
  end

  it "should publish" do
    with_channel do |ch|
      q = ch.queue
      q.publish "hej"
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.body_io.to_s.should eq "hej" if msg
    end
  end

  it "should get" do
    with_channel do |ch|
      q = ch.queue
      q.publish("foo")
      q.publish("bar")
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.not_nil!.message_count.should eq 1
    end
  end

  it "should consume" do
    s = ::Channel(Nil).new
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

  it "should block subscribe" do
    with_channel do |ch|
      q = ch.queue
      tag = "block"
      q.publish("hej!")
      b = false
      q.subscribe(tag: tag, block: true) do |_|
        b.should be_false
        b = true
        q.unsubscribe(tag)
      end
      b.should be_true
    end
  end

  it "should publish msg larger than frame_max" do
    with_channel do |ch|
      q = ch.queue
      str = "a" * 257 * 1024
      q.publish str
      msg = q.get
      msg.should_not be_nil
      msg.body_io.to_s.should eq str if msg
    end
  end

  it "should negotiate frame_max" do
    with_connection(frame_max: 4096_u32) do |c|
      c.frame_max.should eq 4096_u32
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

  it "should delete a queue" do
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

  it "should purge a queue" do
    with_channel do |ch|
      q = ch.queue
      q.publish ""
      ok = q.purge
      ok[:message_count].should eq 1
    end
  end

  it "should declare and publish to an exchange" do
    with_channel do |ch|
      q = ch.queue("crystal-q2")
      x = ch.default_exchange
      x.publish "hej", q.name
      q.delete[:message_count].should eq 1
    end
  end

  it "should publish with confirm" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm("hej").should eq true
      ok = q.delete
      ok[:message_count].should eq 1
    end
  end

  it "should use blocks" do
    with_channel do |ch|
      ch.basic_publish_confirm("hej", "", "my-queue").should eq true
    end
  end

  it "should publish and consume properties" do
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

  it "should get multiple messages" do
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

  it "raises exception on write when the server has closed the connection" do
    with_channel do |ch|
      # rabbitmq doesn't implement client flow
      ch.flow(false)
      sleep 0.1
      # by now we should've gotten the connection closed by the server
      expect_raises(AMQP::Client::Connection::ClosedException) do
        ch.queue
      end
    end
  end

  it "should have many unprocessed confirms" do
    with_channel do |ch|
      q = ch.queue
      ch.confirm_select
      body = IO::Memory.new(0)
      4000.times do
        q.publish body
      end
      d = q.delete
      d[:message_count].should eq 4000
    end
  end

  it "should publish in a consume block" do
    with_channel do |ch|
      tag = "block"
      q = ch.queue
      5.times { q.publish("") }
      b = false
      q.subscribe(tag: tag, no_ack: false, block: true) do |msg|
        q.publish "again"
        msg.ack
        b = true
        q.unsubscribe(tag)
      end
      b.should be_true
    end
  end

  it "should publish IO objects without pos or bytesize" do
    io = IO::Memory.new "abcde"
    sized = IO::Sized.new(io, read_size: 3)
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm(sized, 3).should eq true
      msg = q.get
      msg.should_not be_nil
      msg.not_nil!.body_io.to_s.should eq "abc"
    end
  end

  it "should open all queues" do
    AMQP::Client.start("amqp://localhost/") do |c|
      (1_u16..c.channel_max).each do |id|
        ch = c.channel
        ch.id.should eq id
      end
    end
  end

  it "should set connection name" do
    AMQP::Client.start("amqp://localhost/?name=My+Name") do |_|
      sleep 5 # RabbitMQ is slow
      HTTP::Client.get("http://guest:guest@localhost:15672/api/connections") do |resp|
        conns = JSON.parse resp.body_io
        names = conns.as_a.map { |c| c.dig("client_properties", "connection_name") }
        names.should contain "My Name"
      end
    end
  end

  it "should not wait for connection close" do
    conn = AMQP::Client.new("amqp://localhost/").connect
    conn.close(no_wait: true)
  end

  it "should not drop messages on basic_cancel" do
    with_channel do |ch|
      tag = "block"
      q = ch.queue("basic_cancel")
      5.times { q.publish("") }
      messages_handled = 0
      q.subscribe(tag: tag, no_ack: false, block: true) do |msg|
        msg.ack
        messages_handled += 1
        ch.basic_cancel(tag) if ch.has_subscriber?(tag)
      end
      sleep 0.5
      (q.message_count + messages_handled).should eq 5
      q.delete
    end
  end
end
