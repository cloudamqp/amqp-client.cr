require "./channel"

class AMQP::Client
  class Exchange
    getter name

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String, no_wait = false, args = Arguments.new)
      @channel.exchange_bind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def unbind(exchange : String, routing_key : String, no_wait = false, args = Arguments.new)
      @channel.exchange_unbind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def publish(message, routing_key : String, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish(message, @name, routing_key, mandatory, immediate, props)
    end

    def publish_confirm(message, routing_key : String, mandatory = false, immediate = false, props = Properties.new)
      @channel.basic_publish_confirm(message, @name, routing_key, mandatory, immediate, props)
    end

    def delete(if_unused = false)
      @channel.exchange_delete(@name, if_unused)
    end
  end
end
