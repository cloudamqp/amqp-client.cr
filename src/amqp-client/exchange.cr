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
    def bind(exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new)
      @channel.exchange_bind(@name, exchange, routing_key, no_wait, arguments)
      self
    end

    # Unbind the exchange from another exchange
    def unbind(exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new)
      @channel.exchange_unbind(@name, exchange, routing_key, no_wait, arguments)
      self
    end

    # Publish a message to the exchange
    def publish(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish(message, @name, routing_key, mandatory, immediate, properties)
    end

    # Publish and confirm a message to the exchange
    def publish_confirm(message, routing_key : String, mandatory = false, immediate = false, props properties = Properties.new)
      @channel.basic_publish_confirm(message, @name, routing_key, mandatory, immediate, properties)
    end

    # Delete the exchange
    def delete(if_unused = false)
      @channel.exchange_delete(@name, if_unused)
    end
  end
end
