require "./spec_helper"
{% if Fiber.has_constant?("ExecutionContext") %}
  require "wait_group"
{% end %}

describe "publisher confirms" do
  it "can publish from a confirm callback" do
    with_channel do |ch|
      q = ch.queue
      ch.confirm_select
      confirmed = Channel(String).new(2)
      q.publish("first") do
        q.publish("second") { confirmed.send "second" }
        confirmed.send "first"
      end
      2.times.map do
        select
        when tag = confirmed.receive
          tag
        when timeout(5.seconds)
          fail "confirm callback never ran"
        end
      end.to_set.should eq Set{"first", "second"}
    end
  end

  {% if Fiber.has_constant?("ExecutionContext") %}
    # The connection, and so its read loop, is in an execution context of
    # its own: the read loop handles the broker's acks on another thread
    # than the publishers, as soon as they come
    it "confirms every publish when the read loop runs on another thread" do
      with_connection_in_own_context do |conn|
        ch = conn.channel
        q = ch.queue("", durable: false, exclusive: true)
        publishers = 4
        publishes = 500
        confirmed = Atomic(Int32).new(0)
        wg = WaitGroup.new(publishers)
        publishers.times do
          spawn do
            publishes.times do
              confirmed.add(1) if ch.basic_publish_confirm("", "", q.name)
            end
          ensure
            wg.done
          end
        end
        all_done = Channel(Nil).new
        spawn { wg.wait; all_done.send nil }
        select
        when all_done.receive
        when timeout(30.seconds)
          fail "publish confirms never came (#{confirmed.get} of #{publishers * publishes})"
        end
        confirmed.get.should eq publishers * publishes
      end
    end

    it "returns from wait_for_confirms when the read loop runs on another thread" do
      with_connection_in_own_context do |conn|
        ch = conn.channel
        q = ch.queue("", durable: false, exclusive: true)
        ch.confirm_select
        2000.times do |i|
          q.publish ""
          returned = Channel(Bool).new
          spawn { returned.send ch.wait_for_confirms }
          select
          when ok = returned.receive
            ok.should be_true
          when timeout(5.seconds)
            fail "wait_for_confirms never returned (iteration #{i})"
          end
        end
      end
    end
  {% end %}
end

{% if Fiber.has_constant?("ExecutionContext") %}
  def with_connection_in_own_context(&)
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
      yield conn
    ensure
      conn.close
    end
  end
{% end %}
