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

c = AMQP::Client.new("amqp://guest:guest@localhost").connect
ch = c.channel
q = ch.queue
q.publish "hej"
msg = q.get(no_ack: true)
puts msg.body_io.to_s
c.close
```

## Contributing

1. Fork it (<https://github.com/cloudamqp/amqp-client.cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Carl Hörberg](https://github.com/carlhoerberg) - creator and maintainer
