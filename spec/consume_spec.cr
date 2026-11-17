require "./spec_helper"

describe "basic_consume" do
  it "uses the consumer tag the server generates when none is given" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      tag = q.subscribe { }
      tag.should_not be_empty
      ch.has_subscriber?(tag).should be_true
      q.consumer_count.should eq 1
      q.unsubscribe(tag, no_wait: false)
      q.consumer_count.should eq 0
    end
  end

  it "uses the consumer tag given" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      q.subscribe(tag: "my-tag") { }.should eq "my-tag"
      ch.has_subscriber?("my-tag").should be_true
      q.unsubscribe("my-tag", no_wait: false)
      q.consumer_count.should eq 0
    end
  end

  it "registers each consumer under its own tag" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      tags = Array.new(5) { q.subscribe { } }
      tags.uniq.size.should eq 5
      tags.each { |t| ch.has_subscriber?(t).should be_true }
      q.consumer_count.should eq 5
    end
  end

  it "closes the existing consumer when a basic.consume reusing its tag fails" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      first = Channel(Nil).new(1)
      spawn do
        q.subscribe(tag: "dup", block: true) { }
      rescue AMQP::Client::Channel::ClosedException
      ensure
        first.send nil
      end
      until q.consumer_count == 1
        sleep 10.milliseconds
      end
      expect_raises(AMQP::Client::Channel::ClosedException, /NOT_FOUND/) do
        ch.basic_consume("no-such-queue", tag: "dup") { }
      end
      select
      when first.receive
      when timeout(5.seconds)
        fail "the first consumer was left waiting after the channel closed"
      end
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
    # Each ConsumeOk must go to the basic_consume call that sent its
    # basic.consume, and the consumer must be registered under its tag,
    # whichever of the calls writes its frame first
    it "pairs concurrent basic_consume calls on a channel with their ConsumeOk" do
      with_connection_on_other_thread do |conn|
        ch = conn.channel
        ch.confirm_select
        callers = Fiber::ExecutionContext::Parallel.new("consumers", 2)
        200.times do |i|
          queues = {"a", "b"}.map { |name| ch.queue("", durable: false, exclusive: true).tap(&.publish(name)) }
          ch.wait_for_confirms
          received = Channel({String, String}).new(2)
          returned = Channel({String, String} | Exception).new(2)
          {"a", "b"}.each_with_index do |name, idx|
            callers.spawn do
              tag = queues[idx].subscribe(tag: "#{name}-#{i}") { |msg| received.send({name, msg.body_io.to_s}) }
              returned.send({name, tag})
            rescue ex
              returned.send ex
            end
          end
          2.times do
            within(5.seconds, "basic_consume (iteration #{i})") do
              case r = returned.receive
              when Exception then raise r
              else                r[1].should eq "#{r[0]}-#{i}"
              end
            end
          end
          2.times do
            within(5.seconds, "a delivery (iteration #{i})") do
              consumer, body = received.receive
              body.should eq consumer
            end
          end
          queues.each &.delete
        end
      end
    end
  {% end %}

  {% if Fiber.has_constant?("ExecutionContext") %}
    # The server sends what's in the queue right after ConsumeOk. Here the
    # connection, and so its read loop, is in an execution context of its
    # own: the read loop handles those deliveries while the fiber that sent
    # basic.consume is still being resumed, on another thread.
    it "delivers to a consumer when the read loop runs on another thread" do
      iterations = 100
      messages = 10
      with_connection_on_other_thread do |conn|
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
      end
    end
  {% end %}
end
