require "amq-protocol"
require "./channel"

class AMQP::Client
  abstract struct Message
    getter exchange, routing_key, properties, body_io

    def initialize(@exchange : String, @routing_key : String,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory)
    end
  end

  struct ReturnedMessage < Message
    getter reply_code, reply_text

    def initialize(@reply_code : UInt16, @reply_text : String,
                   @exchange : String, @routing_key : String,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory)
    end
  end

  struct DeliveredMessage < Message
    getter delivery_tag, redelivered

    def initialize(@channel : Channel, @exchange : String,
                   @routing_key : String, @delivery_tag : UInt64,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory, @redelivered : Bool)
    end

    def ack
      @channel.basic_ack(@delivery_tag)
    end

    def nack(requeue = false)
      @channel.basic_nack(@delivery_tag, requeue: requeue)
    end

    def reject(requeue = false)
      @channel.basic_reject(@delivery_tag, requeue: requeue)
    end
  end
end
