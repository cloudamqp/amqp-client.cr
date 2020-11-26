require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    e = ch.exchange_declare("topic_animals", type: "topic")
    q = ch.queue
    animals = ARGV.shift || "anonymous.info"
    q.bind exchange: "topic_animals", routing_key: animals
    msg = ARGV.empty? ? "Hello World!" : ARGV.join(' ')
    ch.basic_publish msg, exchange: "topic_animals", routing_key: animals
    puts "Sent: #{msg}"
  end
end
