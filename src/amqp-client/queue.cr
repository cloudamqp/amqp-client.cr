require "./channel"

class AMQP::Client
  class Queue
    getter name

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new)
      @channel.queue_bind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def publish(message, opts = Hash(String, AMQ::Protocol::Field).new)
      @channel.basic_publish(message, "", @name, opts)
    end

    def publish_confirm(message, opts = Hash(String, AMQ::Protocol::Field).new)
      @channel.basic_publish_confirm(message, "", @name, opts)
    end

    def get(no_ack = true)
      @channel.basic_get(@name, no_ack)
    end

    def subscribe(no_ack = true, exclusive = false,
                  args = Hash(String, AMQ::Protocol::Field).new, &blk : DeliveredMessage -> _)
      @channel.basic_consume(@name, no_ack, exclusive, args, &blk)
      self
    end

    def purge
      @channel.queue_purge(@name)
    end

    def delete(if_unused = false, if_empty = false)
      @channel.queue_delete(@name, if_unused, if_empty)
    end
  end
end
