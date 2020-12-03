require "amqp-client"

abort "Usage: [info] [warning] [error]" if ARGV.empty?

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue
    ARGV.each do |severity|
      q.bind exchange: "direct_logs", routing_key: severity
    end
    puts "Waiting for logs. To exit press CTRL+C"
    q.subscribe(no_ack: false, block: true) do |msg|
      puts "Received: #{msg.body_io.to_s}"
      msg.ack
      puts "Done"
    end
  end
end
