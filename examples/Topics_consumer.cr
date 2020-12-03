require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue
    ARGV.each do |animals|
      q.bind exchange: "topic_animals", routing_key: animals
    end
    puts "Waiting for logs. To exit press CTRL+C"
    q.subscribe(no_ack: false, block: true) do |msg|
      puts "Received: #{msg.body_io.to_s}"
      msg.ack
      puts "Done"
    end
  end
end
