require "amq-protocol"

class AMQP::Client
  struct Message
    getter exchange, routing_key, delivery_tag, properties, body_io

    def initialize(@exchange : String, @routing_key : String, @delivery_tag : UInt64, @properties : AMQ::Protocol::Properties, @body_io : IO)
    end
  end
end
