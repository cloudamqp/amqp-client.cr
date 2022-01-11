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

    # Acknowledge the message
    def ack(multiple = false)
      @channel.basic_ack(@delivery_tag, multiple: multiple)
    end

    # Negatively acknowledge (reject) the message
    def nack(multiple = false, requeue = false)
      @channel.basic_nack(@delivery_tag, multiple: multiple, requeue: requeue)
    end

    # Reject the message
    def reject(requeue = false)
      @channel.basic_reject(@delivery_tag, requeue: requeue)
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
