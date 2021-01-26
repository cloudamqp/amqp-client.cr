require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    e = ch.exchange_declare("direct_logs", type: "direct")
    q = ch.queue
    severity = ARGV.shift || "info"
    q.bind exchange: "direct_logs", routing_key: severity
    msg = ARGV.empty? ? "Hello World!" : ARGV.join(' ')
    e.publish msg, routing_key: severity
    puts "Sent: #{msg}"
  end
end
