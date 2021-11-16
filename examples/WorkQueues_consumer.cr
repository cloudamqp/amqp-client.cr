require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue("hello")
    ch.prefetch(count: 1)
    puts "Waiting for messages. To exit press CTRL+C"
    q.subscribe(no_ack: false, block: true) do |msg|
      puts "Received: #{msg.body_io}"
      msg.ack
      sleep msg.body_io.to_s.count('.')
      puts "Done"
    end
  end
end
