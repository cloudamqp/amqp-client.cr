# amqp-client

An AMQP 0-9-1 client for Crystal.

## Installation

1. Add the dependency to your `shard.yml`:
```yaml
dependencies:
  amqp-client:
    github: cloudamqp/amqp-client.cr
```
2. Run `shards install`

## Usage

```crystal
require "amqp-client"

AMQP::Client.start("amqp://guest:guest@localhost") do |c|
  c.channel do |ch|
    # Always set a prefetch limit before consuming
    ch.prefetch(100)

    # Declare a temporary queue
    q = ch.queue("")

    # Declare a durable queue
    q = ch.queue("my-queue")

    # Subscribe to it, and manually acknowledge messages when processed
    q.subscribe(no_ack: false) do |msg|
      puts "Received: #{msg.body_io.to_s}"
      ch.basic_ack(msg.delivery_tag)
    end

    # publish directly to a queue without confirm (fire and forget)
    q.publish "msg"

    # publish directly to a queue and blocking while waiting for confirm
    q.publish_confirm "msg"

    # publish to any exchange/routing-key (fire and forget)
    ch.basic_publish "msg", exchange: "amq.topic", routing_key: "a"

    # publish to any exchange/routing-key and block while waiting for confirm
    ch.basic_publish_confirm "msg", exchange: "amq.topic", routing_key: "a"

    # When the Channel is in confirm mode a block can be given to the basic_publish
    # method and it will be executed when the message is confirmed by the server
    ch.confirm_select
    ch.basic_publish("msg", "amq.topic", "my.topic") do |ok|
      if ok
        puts "Message is confirmed by the server"
      else
        puts "Message was NOT confirmed by the server"
      end
    end

    # This statement will block until a message has arrived
    # The only way to "escape" the block is to unsubscribe
    q.subscribe(tag: "myconsumer", block: true) do |msg|
      q.unsubscribe("myconsumer")
    end

    # Consume and ack, nack or reject msgs
    ch.basic_consume("queue", tag: "consumer-tag", no_ack: false, exclusive: false, block: false) do |msg|
      case msg.body_io.to_s
      when "ack"
        ch.basic_ack(msg.delivery_tag)
      when "reject"
        ch.basic_reject(msg.delivery_tag, requeue: true)
      when "nack"
        ch.basic_nack(msg.delivery_tag, requeue: true, multiple: true)
      end
    end

    ch.prefetch(count: 1000) # alias for basic_qos

    name, message_count, consumer_count =
      ch.queue_declare(name: "myqueue", passive: false, durable: true,
                       exclusive: false, auto_delete: false)
    q = ch.queue # temporary queue that is deleted when the channel is closed
    ch.queue_purge("myqueue")
    ch.queue_bind("myqueue", "amq.topic", "routing-key")
    ch.queue_unbind("myqueue", "amq.topic", "routing-key")
    msg = ch.basic_get("myqueue", no_ack: true)
    ch.basic_ack(msg.delivery_tag)
    ch.queue_delete("myqueue")
    ch.exchange_declare("my-exchange", type: "topic")
    ch.exchange_delete("my-exchange")
  end
end
```

## Connection close and network failures

The client reports a lost connection through two callbacks:

- The client calls `Connection#on_close` only when the broker sends an AMQP
  connection close frame.
- The client calls `Connection#on_disconnect` when the TCP, TLS, or WebSocket
  read fails without an AMQP close frame. Examples of such failures are a
  timeout, a network partition, and a TLS error. The callback receives the
  transport exception.

If a transport failure occurs, the background read loop marks the connection
closed, closes the channels and the consumers, calls `on_disconnect`, and
exits. The read loop does not call `on_close`.

Use `on_disconnect` to unblock a consumer that waits on an application-owned
shutdown channel:

```crystal
require "amqp-client"

AMQP::Client.start("amqp://guest:guest@localhost") do |c|
  shutdown = ::Channel(Nil).new
  Signal::INT.trap { shutdown.close rescue nil }

  c.on_disconnect do |ex|
    puts "Connection lost: #{ex.message}"
    shutdown.close rescue nil
  end

  ch = c.channel
  q = ch.queue("events")
  q.subscribe(no_ack: false) do |msg|
    puts "Received: #{msg.body_io}"
    msg.ack
  end

  shutdown.receive?
end
```

`Connection#closed?` returns `true` after the connection closes for any
reason. If you do not use the callbacks, you can poll `closed?`.

You can consume [stream queues](https://www.rabbitmq.com/streams.html) too: 

```crystal
require "amqp-client"

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
```

## Logging

The client logs to the `amqp.client`, `amqp.client.connection` and
`amqp.client.channel` sources. Connection log lines carry a `connection`
metadata key so lines from several connections in the same application can be
told apart:

```crystal
Log.setup_from_env(default_level: :debug)
AMQP::Client.start("amqp://guest:guest@localhost") do |c|
  c.channel
end
```

```
amqp.client.connection: sent AMQ::Protocol::Frame::Channel::Open(...) -- connection: "127.0.0.1:47182 -> 127.0.0.1:5672"
amqp.client.connection: recv AMQ::Protocol::Frame::Channel::OpenOk(...) -- connection: "127.0.0.1:47182 -> 127.0.0.1:5672"
```

The default is the socket's local and remote address, which identifies the TCP
connection uniquely and can be matched against broker logs and packet captures.
UNIX sockets and WebSockets have no such address pair, so those get a sequence
number instead. Pass `log_id` (or the `log_id` URI parameter) to use your own
identifier, and read it back with `Connection#log_id`:

```crystal
AMQP::Client.start("amqp://localhost?log_id=order-publisher") do |c|
  c.log_id # => "order-publisher"
end
```

## Performance

1-byte messages, without properties/headers:

| Publish rate | Consume rate |
| ------------ | ------------ |
| 1.200.000 msgs/s | 1.000.000 msgs/s |

## Contributing

1. [Fork it](https://github.com/cloudamqp/amqp-client.cr/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Carl Hörberg](https://github.com/carlhoerberg) - creator and maintainer
- [Anders Bälter](https://github.com/baelter)
