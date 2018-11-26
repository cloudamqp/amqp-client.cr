require "../amqp-client"

class AMQP::Client
  class Channel
    def initialize(@client : AMQP::Client, @id : UInt16)
    end
  end
end

