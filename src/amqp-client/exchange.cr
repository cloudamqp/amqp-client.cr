require "./channel"

class AMQP::Client
  # High-level Exchange object
  class Exchange
    # Name of the exchange
    getter name

    # :nodoc:
    def initialize(@channel : Channel, @name : String)
    end

    # Bind the exchange to another exchange
    # self is the destination exchange and the argument is the source exchange
    def bind(exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new)
      @channel.exchange_bind(exchange, @name, routing_key, no_wait, arguments)
      self
    end

    def bind(exchange : String, routing_key : String, no_wait = false, args arguments : NamedTuple = NamedTuple.new)
      bind(exchange, routing_key, no_wait, Arguments.new(arguments))
    end

    # Unbind the exchange from another exchange
    # self is the destination exchange and the argument is the source exchange
    def unbind(exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new)
      @channel.exchange_unbind(exchange, @name, routing_key, no_wait, arguments)
      self
    end

    def unbind(exchange : String, routing_key : String, no_wait = false, args arguments : NamedTuple = NamedTuple.new)
      unbind(exchange, routing_key, no_wait, Arguments.new(arguments))
    end

    # Publish a message to the exchange
    def publish(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish(message, @name, routing_key, mandatory, immediate, properties)
    end

    # Publish and confirm a message to the exchange
    def publish_confirm(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish_confirm(message, @name, routing_key, mandatory, immediate, properties)
    end

    # Publish a message to the exchange, block is called when message is confirmed
    def publish(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new, &blk)
      @channel.basic_publish(message, @name, routing_key, mandatory, immediate, properties, blk)
    end

    # Publish and confirm a message to the exchange, block is called when message is confirmed
    def publish_confirm(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new, &blk)
      @channel.basic_publish_confirm(message, @name, routing_key, mandatory, immediate, properties, blk)
    end

    # Delete the exchange
    def delete(if_unused = false)
      @channel.exchange_delete(@name, if_unused)
    end
  end
end
