require "../src/amqp-client"

AMQP::Client.start do |c|
  c.channel do |ch|
    # prefetch required when consuming from stream queues
    ch.prefetch(10)
    # declare a stream queue using the x-queue-type argument
    q = ch.queue("stream1", args: AMQP::Client::Arguments.new({"x-queue-type": "stream"}))
    puts "Waiting for messages. To exit press CTRL+C"
    # Decide from where to subscribe using the x-stream-offset argument
    q.subscribe(block: true, no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
      puts "Received: #{msg.body_io}"
      msg.ack
    end
  end
end
