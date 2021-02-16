require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    e = ch.exchange("logs", type: "fanout")
    q = ch.queue
    q.bind exchange: "logs", routing_key: ""
    msg = ARGV.empty? ? "Hello World!" : ARGV.join(' ')
    e.publish msg
    puts "Sent: #{msg}"
  end
end
