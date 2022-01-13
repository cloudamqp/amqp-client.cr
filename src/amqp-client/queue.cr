require "./channel"

class AMQP::Client
  # High-level representation of a Queue
  #
  # Example:
  # ```
  # client = AMQP::Client.new.connect
  # ch = client.channel
  # q = ch.queue("foobar")
  # q.bind("amq.fanout", "")
  # q.subscribe do |msg|
  #   puts msg.body_io
  # end
  # ```
  class Queue
    # Name of the queue
    getter name

    # :nodoc:
    def initialize(@channel : Channel, @name : String)
    end

    # Bind the queue to an exchange
    def bind(exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new)
      @channel.queue_bind(@name, exchange, routing_key, no_wait, arguments)
      self
    end

    # Unbind the queue from an exchange
    def unbind(exchange : String, routing_key : String, args arguments = Arguments.new)
      @channel.queue_unbind(@name, exchange, routing_key, arguments)
      self
    end

    # Publish a message directly to the queue
    def publish(message, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish(message, "", @name, mandatory, immediate, properties)
    end

    # Publish a message directly to the queue
    def publish(io : IO, bytesize : Int, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish(io, bytesize, "", @name, mandatory, immediate, properties)
    end

    # Publish and confirm a message directly to the queue
    def publish_confirm(message, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish_confirm(message, "", @name, mandatory, immediate, properties)
    end

    # Publish and confirm a message directly to the queue
    def publish_confirm(io : IO, bytesize : Int, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish_confirm(io, bytesize, "", @name, mandatory, immediate, properties)
    end

    # Poll the queue for a message
    #
    # See `Channel#basic_get`
    def get(no_ack = true)
      @channel.basic_get(@name, no_ack)
    end

    # Consume messages from the queue
    #
    # See `Channel#basic_consume`
    def subscribe(tag = "", no_ack = true, exclusive = false, block = false,
                  args arguments = Arguments.new, work_pool = 1, &blk : DeliverMessage -> Nil)
      @channel.basic_consume(@name, tag, no_ack, exclusive, block, arguments, work_pool, &blk)
    end

    # Unsubscribe from the queue
    #
    # See `Channel#basic_cancel` for more details
    def unsubscribe(consumer_tag, no_wait = true)
      @channel.basic_cancel(consumer_tag, no_wait)
      self
    end

    # Purge the queue
    #
    # See `Channel#queue_purge`
    def purge
      @channel.queue_purge(@name)
    end

    # Delete the queue
    #
    # See `Channel#queue_delete`
    def delete(if_unused = false, if_empty = false)
      @channel.queue_delete(@name, if_unused, if_empty)
    end

    # Check number of messages in the queue
    def message_count
      @channel.queue_declare(@name, passive: true)[:message_count]
    end

    # Check number of consumers of the queue
    def consumer_count
      @channel.queue_declare(@name, passive: true)[:consumer_count]
    end
  end
end
