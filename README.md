[![Build Status](https://travis-ci.org/cloudamqp/amqp-client.cr.svg?branch=master)](https://travis-ci.org/cloudamqp/amqp-client.cr)

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
    q = ch.queue("my-queue")
    q.subscribe(no_ack: false) do |msg|
      puts "Received: #{msg.body_io.to_s}"
      ch.basic_ack(msg.delivery_tag)
    end

    # publish directly to a queue without confirm
    q.publish "msg"

    # publish directly to a queue, blocking while waiting for confirm
    q.publish_confirm "msg"

    # publish to any exchange/routing-key
    ch.basic_publish "msg", exchange: "amq.topic", routing_key: "a"

    # publish to any exchange/routing-key and wait for confirm
    ch.basic_publish_confirm "msg", exchange: "amq.topic", routing_key: "a"
  end
end
```

## Contributing

1. Fork it (<https://github.com/cloudamqp/amqp-client.cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Carl Hörberg](https://github.com/carlhoerberg) - creator and maintainer
- [Anders Bälter](https://github.com/baelter)
