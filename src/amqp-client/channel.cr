require "./connection"
require "./message"

class AMQP::Client
  class Channel
    getter id

    def initialize(@connection : Connection, @id : UInt16)
      @incoming = ::Channel(AMQ::Protocol::Frame).new
    end

    def open
      @connection.write AMQ::Protocol::Frame::Channel::Open.new(@id)
      next_frame.as?(AMQ::Protocol::Frame::Channel::OpenOk) || raise "Unexpected frame"
      self
    end

    def close
      @incoming.close
    end

    def incoming(frame)
      @incoming.send frame
    end

    private def next_frame : AMQ::Protocol::Frame
      f = @incoming.receive
      case f
      when AMQ::Protocol::Frame::Channel::Close
        close
        raise "Channel unexpectedly closed"
      else
        return f
      end
    end

    def queue
      queue("", durable: false, auto_delete: true, exclusive: true)
    end

    def queue(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, no_wait = false, args = Hash(String, Field).new)
      @connection.write AMQ::Protocol::Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable, exclusive, auto_delete, no_wait, args)
      f = next_frame.as?(AMQ::Protocol::Frame::Queue::DeclareOk) || raise "Unexpected frame"
      f.name
    end

    def publish(io : IO, exchange : String, routing_key : String, opts = {} of String => String)
      @connection.write AMQ::Protocol::Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, false, false), flush: false
      @connection.write AMQ::Protocol::Frame::Header.new(@id, 60_u16, 0_u16, io.bytesize.to_u64, AMQ::Protocol::Properties.new), flush: false
      @connection.write AMQ::Protocol::Frame::Body.new(@id, io.bytesize.to_u32, io)
    end

    def get(queue : String, no_ack : Bool) : Message?
      @connection.write AMQ::Protocol::Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      f = next_frame
      case f
      when AMQ::Protocol::Frame::Basic::GetEmpty
        return nil
      when AMQ::Protocol::Frame::Basic::GetOk
        header = next_frame.as?(AMQ::Protocol::Frame::Header) || raise "Unexpected frame"
        body = next_frame.as?(AMQ::Protocol::Frame::Body) || raise "Unexpected frame"
        Message.new(f.exchange, f.routing_key, f.delivery_tag, header.properties, body.body)
      else
        raise "Unexpected frame"
      end
    end

    def consume(queue, no_ack, exclusive, no_wait, arguments)
      @connection.write AMQ::Protocol::Frame::Basic::Consume.new(@id, 0_u16, queue, consumer_tag, false, no_ack, exclusive, no_wait, arguments)
      next_frame.as?(AMQ::Protocol::Frame::Basic::ConsumeOk) || raise "Unexpected frame" unless no_wait
    end

    def ack(delivery_tag : UInt64, multiple = false) : Nil
      @connection.write AMQ::Protocol::Frame::Basic::Ack.new(@id, delivery_tag, multiple)
    end

    def reject(delivery_tag : UInt64, requeue = false) : Nil
      @connection.write AMQ::Protocol::Frame::Basic::Reject.new(@id, delivery_tag, requeue)
    end

    def nack(delivery_tag : UInt64, multiple = false, requeue = false) : Nil
      @connection.write AMQ::Protocol::Frame::Basic::Reject.new(@id, delivery_tag, multiple, requeue)
    end

    def prefetch(count, global = false) : Nil
      @connection.write AMQ::Protocol::Frame::Basic::Qos.new(@id, 0_u32, count, global)
      next_frame.as?(AMQ::Protocol::Frame::Basic::QosOk) || raise "Unexpected frame"
    end
  end
end
