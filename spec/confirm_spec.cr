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
    # The read loop, which handles the confirms, runs on a thread of its own
    # while the publishers run on the others
    it "confirms every publish when the client runs on several threads" do
      publishers = 8
      publishes = 500
      done = Channel(Exception | Int32).new
      ctx = Fiber::ExecutionContext::Parallel.new("confirms", 4)
      ctx.spawn do
        with_channel do |ch|
          q = ch.queue
          wg = WaitGroup.new(publishers)
          confirmed = Atomic(Int32).new(0)
          publishers.times do
            ctx.spawn do
              publishes.times do
                confirmed.add(1) if ch.basic_publish_confirm("", "", q.name)
              end
            ensure
              wg.done
            end
          end
          wg.wait
          done.send confirmed.get
        end
      rescue ex
        done.send ex
      end
      select
      when result = done.receive
        raise result if result.is_a?(Exception)
        result.should eq publishers * publishes
      when timeout(30.seconds)
        fail "publish confirms never came"
      end
    end

    it "returns from wait_for_confirms when the client runs on several threads" do
      waiters = 4
      done = Channel(Exception?).new
      ctx = Fiber::ExecutionContext::Parallel.new("wait-for-confirms", 4)
      ctx.spawn do
        with_channel do |ch|
          q = ch.queue
          ch.confirm_select
          200.times do
            # A few publishes at a time, so the last confirm can come while
            # the waiters check whether there's anything to wait for
            3.times { q.publish "" }
            wg = WaitGroup.new(waiters)
            returned = Atomic(Int32).new(0)
            waiters.times do
              ctx.spawn do
                returned.add(1) if ch.wait_for_confirms
              ensure
                wg.done
              end
            end
            wg.wait
            returned.get.should eq waiters
          end
        end
        done.send nil
      rescue ex
        done.send ex
      end
      select
      when ex = done.receive
        raise ex if ex
      when timeout(30.seconds)
        fail "wait_for_confirms never returned"
      end
    end
  {% end %}
end
