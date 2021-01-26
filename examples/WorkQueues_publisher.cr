require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    ch.queue_declare(name: "hello", durable: true)
    ch.queue_bind("hello", "amq.direct", "hello")
    msg = ARGV.empty? ? "Hello World!" : ARGV.join(' ')
    ch.basic_publish msg, exchange: "amq.direct", routing_key: "hello", props: AMQP::Client::Properties.new(delivery_mode: 2)
    puts "Sent: #{msg}"
  end
end
