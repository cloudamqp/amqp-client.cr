require "amq-protocol"
require "./channel"

class AMQP::Client
  abstract struct BaseMessage
    getter exchange, routing_key, properties, body_io

    def initialize(@exchange : String, @routing_key : String,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory)
    end
  end

  struct ReturnedMessage < BaseMessage
    getter reply_code, reply_text

    def initialize(@reply_code : UInt16, @reply_text : String,
                   @exchange : String, @routing_key : String,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory)
    end
  end

  abstract struct Message < BaseMessage
    getter delivery_tag, redelivered

    def initialize(@channel : Channel, @exchange : String,
                   @routing_key : String, @delivery_tag : UInt64,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory, @redelivered : Bool)
    end

    def ack(multiple = false)
      @channel.basic_ack(@delivery_tag, multiple: multiple)
    end

    def nack(multiple = false, requeue = false)
      @channel.basic_nack(@delivery_tag, multiple: multiple, requeue: requeue)
    end

    def reject(requeue = false)
      @channel.basic_reject(@delivery_tag, requeue: requeue)
    end
  end

  struct DeliverMessage < Message
  end

  struct GetMessage < Message
    getter message_count

    def initialize(@channel : Channel, @exchange : String,
                   @routing_key : String, @delivery_tag : UInt64,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory, @redelivered : Bool, @message_count : UInt32)
    end
  end
end
