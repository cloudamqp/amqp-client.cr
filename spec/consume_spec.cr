require "./spec_helper"

describe "basic_consume" do
  it "generates a consumer tag when none is given" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      tag = q.subscribe { }
      tag.should start_with "amq.ctag-"
      q.consumer_count.should eq 1
      q.unsubscribe(tag, no_wait: false)
      q.consumer_count.should eq 0
    end
  end

  it "uses the consumer tag given" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      q.subscribe(tag: "my-tag") { }.should eq "my-tag"
      q.unsubscribe("my-tag", no_wait: false)
      q.consumer_count.should eq 0
    end
  end

  it "delivers to a consumer whose ConsumeOk is followed by a delivery" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      q.publish_confirm "m"
      received = Channel(String).new(1)
      q.subscribe { |msg| received.send msg.body_io.to_s }
      select
      when body = received.receive
        body.should eq "m"
      when timeout(5.seconds)
        fail "the delivery never reached the consumer"
      end
    end
  end

  {% if Fiber.has_constant?("ExecutionContext") %}
    # The server sends what's in the queue right after ConsumeOk. Here the
    # connection, and so its read loop, is in an execution context of its
    # own: the read loop handles those deliveries while the fiber that sent
    # basic.consume is still being resumed, on another thread.
    it "delivers to a consumer when the read loop runs on another thread" do
      iterations = 100
      messages = 10
      reader = Fiber::ExecutionContext::Concurrent.new("amqp read loop")
      connected = Channel(AMQP::Client::Connection | Exception).new
      reader.spawn do
        connected.send AMQP::Client.new.connect
      rescue ex
        connected.send ex
      end
      conn = connected.receive
      raise conn if conn.is_a?(Exception)
      begin
        ch = conn.channel
        q = ch.queue("", durable: false, exclusive: true)
        ch.confirm_select
        iterations.times do |i|
          # In the queue before the consumer is
          messages.times { q.publish "" }
          ch.wait_for_confirms
          received = Channel(Nil).new(messages)
          tag = q.subscribe(no_ack: true) { received.send nil }
          messages.times do
            select
            when received.receive
            when timeout(5.seconds)
              fail "a delivery never reached the consumer (iteration #{i})"
            end
          end
          q.unsubscribe(tag, no_wait: false)
        end
      ensure
        conn.close
      end
    end
  {% end %}
end
