require "./spec_helper"

describe "publisher confirms" do
  it "calls a confirm block that publishes on the same channel" do
    with_channel do |ch|
      q = ch.queue("", durable: false, exclusive: true)
      ch.confirm_select
      published = Channel(Bool).new(1)
      ch.basic_publish("1", "", q.name) do
        ch.basic_publish("2", "", q.name)
        published.send true
      rescue ex
        published.send false
        raise ex
      end
      within(5.seconds, "the confirm block") do
        published.receive.should be_true
      end
      within(5.seconds, "wait_for_confirms") { ch.wait_for_confirms }
      q.message_count.should eq 2
    end
  end

  {% if Fiber.has_constant?("ExecutionContext") %}
    # The read loop processes confirms on another thread, while the
    # publishing fiber is still between writing a publish and waiting for it
    describe "with the read loop on another thread" do
      it "returns from wait_for_confirms" do
        with_connection_on_other_thread do |conn|
          ch = conn.channel
          q = ch.queue("", durable: false, exclusive: true)
          ch.confirm_select
          1000.times do |i|
            q.publish ""
            within(5.seconds, "wait_for_confirms (iteration #{i})") do
              ch.wait_for_confirms.should be_true
            end
          end
        end
      end

      it "returns from basic_publish_confirm" do
        with_connection_on_other_thread do |conn|
          ch = conn.channel
          q = ch.queue("", durable: false, exclusive: true)
          1000.times do |i|
            within(5.seconds, "basic_publish_confirm (iteration #{i})") do
              ch.basic_publish_confirm("", "", q.name).should be_true
            end
          end
        end
      end

      # A confirm block runs on the read loop, and publishing from it takes
      # the connection's write lock. Other publishers take the write lock and
      # then the channel's confirm lock, so the read loop mustn't hold the
      # confirm lock while calling blocks.
      it "doesn't deadlock when confirm blocks publish while other fibers publish" do
        with_connection_on_other_thread do |conn|
          ch = conn.channel
          q = ch.queue("", durable: false, exclusive: true)
          ch.confirm_select
          count = 5_000
          called = Channel(Nil).new(count)
          publishers = Fiber::ExecutionContext::Parallel.new("publishers", 3)
          2.times do
            publishers.spawn { count.times { ch.basic_publish("", "", q.name) } }
          end
          publishers.spawn do
            count.times do
              ch.basic_publish("", "", q.name) do
                ch.basic_publish("", "", q.name)
                called.send nil
              end
            end
          end
          count.times do |i|
            select
            when called.receive
            when timeout(5.seconds)
              fail "deadlocked after #{i} of #{count} confirm blocks"
            end
          end
        end
      end

      it "calls the confirm block of every publish" do
        with_connection_on_other_thread do |conn|
          ch = conn.channel
          q = ch.queue("", durable: false, exclusive: true)
          count = 10_000
          confirmed = Channel(Bool).new(count)
          count.times do
            ch.basic_publish("", "", q.name) { |ok| confirmed.send ok }
          end
          count.times do |i|
            select
            when ok = confirmed.receive
              ok.should be_true
            when timeout(5.seconds)
              fail "#{count - i} of #{count} confirm blocks were never called"
            end
          end
        end
      end
    end
  {% end %}
end
