require "amqp-client"

AMQP::Client.start("amqps://user:password@hostname/vhost") do |c|
  c.channel do |ch|
    q = ch.queue("hello")
    q.publish_confirm "Hello World!"
    puts "Sent: Hello World!"
  end
end
