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

  it "raises on a connection the client closed" do
    with_connection do |c|
      q = c.channel.queue("", durable: false, exclusive: true)
      c.close
      result = ::Channel(Exception?).new(1)
      spawn do
        q.subscribe { }
        result.send nil
      rescue ex
        result.send ex
      end
      select
      when ex = result.receive
        ex.should be_a AMQP::Client::Connection::ClosedException
      when timeout(1.second)
        fail "subscribe didn't return"
      end
    end
  end

  it "raises when the client closes the connection while waiting for ConsumeOk" do
    with_connection do |c|
      q = c.channel.queue("", durable: false, exclusive: true)
      result = ::Channel(Exception?).new(1)
      spawn do
        q.subscribe { }
        result.send nil
      rescue ex
        result.send ex
      end
      Fiber.yield
      c.close(no_wait: true)
      select
      when ex = result.receive
        ex.should be_a AMQP::Client::Connection::ClosedException
      when timeout(1.second)
        fail "subscribe didn't return"
      end
    end
  end

  it "doesn't log an error for an ack after the client closed the channel" do
    backend = ::Log::MemoryBackend.new
    Log.builder.bind "amqp.client.channel", :debug, backend
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      q.publish "m"
      received = ::Channel(Nil).new
      acked = ::Channel(Nil).new
      q.subscribe(no_ack: false) do |msg|
        received.send nil
        received.send nil
        msg.ack
      ensure
        acked.send nil
      end
      received.receive
      ch.close
      received.receive
      acked.receive
      Fiber.yield
    end
    backend.entries.select(&.severity.error?).map(&.message).should be_empty
  ensure
    Log.builder.unbind "amqp.client.channel", :debug, backend if backend
  end

  it "returns from a blocking subscribe when the client closes the connection" do
    with_connection do |c|
      q = c.channel.queue("", durable: false, exclusive: true)
      result = ::Channel(Exception?).new(1)
      spawn do
        q.subscribe(block: true) { }
        result.send nil
      rescue ex
        result.send ex
      end
      until q.consumer_count == 1
        sleep 10.milliseconds
      end
      c.close
      within(5.seconds, "subscribe") { result.receive.should be_nil }
    end
  end

  it "returns from a blocking subscribe when the client closes the channel" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      result = ::Channel(Exception?).new(1)
      spawn do
        q.subscribe(block: true) { }
        result.send nil
      rescue ex
        result.send ex
      end
      until q.consumer_count == 1
        sleep 10.milliseconds
      end
      ch.close
      within(5.seconds, "subscribe") { result.receive.should be_nil }
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

    it "sends every ack when threads ack and publish on one connection" do
      count = 9_000
      with_connection_on_other_thread do |conn|
        ch = conn.channel
        ch.prefetch(300)
        q = ch.queue("", durable: false, exclusive: true)
        deliveries = Channel(AMQP::Client::DeliverMessage).new(count)
        q.subscribe(no_ack: false) { |msg| deliveries.send msg }
        workers = Fiber::ExecutionContext::Parallel.new("publishers and ackers", 4)
        acked = WaitGroup.new(count)
        3.times do
          workers.spawn { (count // 3).times { q.publish "" } }
        end
        3.times do
          workers.spawn do
            while msg = deliveries.receive?
              msg.ack
              acked.done
            end
          end
        end
        within(20.seconds, "#{count} deliveries acked") { acked.wait }
        deliveries.close
        # Unacked messages would go back to the queue
        ch.close
        conn.channel.queue_declare(q.name, passive: true)[:message_count].should eq 0
      end
    end
  {% end %}
end
