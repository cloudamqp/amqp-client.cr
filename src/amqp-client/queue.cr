require "./channel"

class AMQP::Client
  class Queue
    getter name

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String, args = Hash(String, AMQ::Protocol::Field).new)
      @channel.queue_bind(@name, exchange, routing_key, args)
      self
    end

    def publish(message, opts = Hash(String, AMQ::Protocol::Field).new)
      @channel.basic_publish(message, "", @name, opts)
    end

    def get(no_ack = true)
      @channel.basic_get(@name, no_ack)
    end

    def subscribe(no_ack = true, exclusive = false,
                  args = Hash(String, AMQ::Protocol::Field).new, &blk : Message -> _)
      @channel.basic_consume(@name, no_ack, exclusive, args, &blk)
      self
    end
  end
end
