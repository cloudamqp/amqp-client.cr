require "./channel"

class AMQP::Client
  class Exchange
    getter name

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new)
      @channel.exchange_bind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def unbind(exchange : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new)
      @channel.exchange_unbind(@name, exchange, routing_key, no_wait, args)
      self
    end

    def publish(message, routing_key : String, opts = Hash(String, AMQ::Protocol::Field).new)
      @channel.basic_publish(message, @name, routing_key, opts)
    end

    def delete(if_unused = false)
      @channel.exchange_delete(@name, if_unused)
    end
  end
end
