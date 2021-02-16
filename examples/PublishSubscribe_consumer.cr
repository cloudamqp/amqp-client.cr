require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue
    q.bind exchange: "logs", routing_key: ""
    puts "Waiting for logs. To exit press CTRL+C"
    q.subscribe(no_ack: false, block: true) do |msg|
      puts "Received: #{msg.body_io.to_s}"
      msg.ack
      sleep msg.body_io.to_s.count('.')
      puts "Done"
    end
  end
end
