require "./channel"

class AMQP::Client
  class Queue
    getter name

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String, no_wait = false, args = Arguments.new)
      @channel.queue_bind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def unbind(exchange : String, routing_key : String, args = Arguments.new)
      @channel.queue_unbind(@name, exchange, routing_key, args)
      self
    end

    def publish(message, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish(message, "", @name, mandatory, immediate, props)
    end

    def publish(io : IO, bytesize : Int, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish(io, bytesize, "", @name, mandatory, immediate, props)
    end

    def publish_confirm(message, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish_confirm(message, "", @name, mandatory, immediate, props)
    end

    def publish_confirm(io : IO, bytesize : Int, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish_confirm(io, bytesize, "", @name, mandatory, immediate, props)
    end

    def get(no_ack = true)
      @channel.basic_get(@name, no_ack)
    end

    def subscribe(tag = "", no_ack = true, exclusive = false, block = false,
                  args = Arguments.new, work_pool = 1, &blk : DeliverMessage -> Nil)
      @channel.basic_consume(@name, tag, no_ack, exclusive, block, args, work_pool, &blk)
    end

    def unsubscribe(consumer_tag)
      @channel.basic_cancel(consumer_tag)
    end

    def purge
      @channel.queue_purge(@name)
    end

    def delete(if_unused = false, if_empty = false)
      @channel.queue_delete(@name, if_unused, if_empty)
    end

    def message_count
      @channel.queue_declare(@name, passive: true)[:message_count]
    end

    def consumer_count
      @channel.queue_declare(@name, passive: true)[:consumer_count]
    end
  end
end
