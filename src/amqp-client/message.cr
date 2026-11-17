require "amq-protocol"
require "./channel"

class AMQP::Client
  # Message returned on publish by the server
  struct ReturnedMessage
    getter reply_code, reply_text, exchange, routing_key, properties, body_io

    # :nodoc:
    def initialize(@reply_code : UInt16, @reply_text : String,
                   @exchange : String, @routing_key : String,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory)
    end
  end

  # Abstract message, use both for Get and Delivered messages
  abstract struct Message
    getter exchange, routing_key, properties, body_io, delivery_tag, redelivered

    # :nodoc:
    def initialize(@channel : Channel, @exchange : String,
                   @routing_key : String, @delivery_tag : UInt64,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory, @redelivered : Bool)
    end

    # Acknowledge the message, see `Channel#basic_ack` for *flush*
    def ack(multiple = false, flush : Bool = true)
      @channel.basic_ack(@delivery_tag, multiple: multiple, flush: flush)
    end

    # Negatively acknowledge (reject) the message, see `Channel#basic_ack` for *flush*
    def nack(multiple = false, requeue = false, flush : Bool = true)
      @channel.basic_nack(@delivery_tag, multiple: multiple, requeue: requeue, flush: flush)
    end

    # Reject the message, see `Channel#basic_ack` for *flush*
    def reject(requeue = false, flush : Bool = true)
      @channel.basic_reject(@delivery_tag, requeue: requeue, flush: flush)
    end
  end

  # Message delivered to a consumer
  struct DeliverMessage < Message
  end

  # Message retrived by `Channel#basic_get`
  struct GetMessage < Message
    getter message_count

    # :nodoc:
    def initialize(@channel : Channel, @exchange : String,
                   @routing_key : String, @delivery_tag : UInt64,
                   @properties : AMQ::Protocol::Properties,
                   @body_io : IO::Memory, @redelivered : Bool,
                   @message_count : UInt32)
    end
  end
end
