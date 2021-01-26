require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue("hello")
    puts "Waiting for messages. To exit press CTRL+C"
    q.subscribe(block: true) do |msg|
      puts "Received: #{msg.body_io.to_s}"
    end
  end
end
