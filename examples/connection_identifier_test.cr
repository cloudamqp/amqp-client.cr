require "../src/amqp-client"

# This example demonstrates that multiple connections have unique identifiers in logs
# Run with: LOG_LEVEL=DEBUG crystal run examples/connection_identifier_test.cr

Log.setup_from_env(default_level: :debug)

puts "Creating first connection..."
c1 = AMQP::Client.new.connect
puts "First connection created with ID: #{c1.@connection_id}"

puts "\nCreating second connection..."
c2 = AMQP::Client.new.connect
puts "Second connection created with ID: #{c2.@connection_id}"

puts "\nConnection IDs should be different:"
puts "Connection 1 ID: #{c1.@connection_id}"
puts "Connection 2 ID: #{c2.@connection_id}"
puts "Are they different? #{c1.@connection_id != c2.@connection_id}"

puts "\nPublishing messages on both connections (check debug logs for connection_id)..."
ch1 = c1.channel
ch2 = c2.channel

q1 = ch1.queue
q2 = ch2.queue

q1.publish "Message from connection 1"
q2.publish "Message from connection 2"

puts "\nConsuming messages..."
if msg = q1.get(no_ack: true)
  puts "Got message from connection 1: #{msg.body_io.to_s}"
end

if msg = q2.get(no_ack: true)
  puts "Got message from connection 2: #{msg.body_io.to_s}"
end

puts "\nClosing connections..."
c1.close
c2.close

puts "Done! Check the debug logs above to see connection_id in the log context."
