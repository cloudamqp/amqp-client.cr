# Introduction
These examples cover the basics of an AMQP 0-9-1 client for Crystal. You can install your broker locally, or create a hosted broker with CloudAMQP. Just replace the connection URL with the one for your broker. 

## Installation

1. Add the dependency to your `shard.yml`:
```yaml
dependencies:
  amqp-client:
    github: cloudamqp/amqp-client.cr
```
2. Run `shards install`

## “Hello World!”

In this example we will write two small programs in Crystal; a publisher that sends a message, and a consumer that receives messages and prints them out.

## Work Queues

In this example we will create a work queue to distribute time-consuming tasks among multiple consumers. Each work will be delivered to exactly one consumer. If we are building up a backlog of work, we can easily scale by adding more consumers.

## Publish/Subscribe 

In this example we will publish a message to be delivered to multiple consumers by using a fanout exchange. 

## Routing

In this example we will publish a message to a direct exchange to be delivered to selective consumers by using routing keys. Add what routing key you want for your consumer and then publish messages with any of those routing keys.
Publish a message with crystal Routing_publisher.cr routing_key message, for example crystal Routing_publisher.cr error “This is an error message” and it will be routed to the consumer with that binding.

## Topics

In this example we will publish a message to a topic exchange to be delivered to selective consumers based on multiple criteria. 
`*` (star) can substitute for exactly one word.
`#` (hash) can substitute for zero or more words.

Add what routing key you want for your consumer with stars and hashes, and then publish messages with any of those routing keys. 
Publish a message with crystal Topics_publisher.cr routing_key message, for example crystal Topics_publisher.cr lazy-rabbit “This is a lazy rabbit” and it will be routed to the consumer with that binding. 
