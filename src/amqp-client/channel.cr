require "./connection"
require "./message"
require "uuid"

class AMQP::Client
  class Channel
    getter id

    def initialize(@connection : Connection, @id : UInt16)
      @incoming = ::Channel(AMQ::Protocol::Frame).new
      @delivery = ::Channel(AMQ::Protocol::Frame::Basic::Deliver).new
      spawn delivery_loop, name: "Channel #{@id} delivery_loop"
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
      case frame
      when AMQ::Protocol::Frame::Basic::Deliver
        @delivery.send frame
      else
        @incoming.send frame
      end
    end

    private def delivery_loop
      loop do
        f = @delivery.receive
        header = next_frame.as?(AMQ::Protocol::Frame::Header) || raise "Unexpected frame"
        body_io = IO::Memory.new(header.body_size)
        until body_io.pos == header.body_size
          body = next_frame.as?(AMQ::Protocol::Frame::Body) || raise "Unexpected frame"
          IO.copy(body.body, body_io, body.body_size)
        end
        body_io.rewind
        msg = Message.new(f.exchange, f.routing_key, f.delivery_tag, header.properties, body_io)
        @consumers[f.consumer_tag].call(msg)
      end
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

    def queue(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new)
      @connection.write AMQ::Protocol::Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable, exclusive, auto_delete, no_wait, args)
      f = next_frame.as?(AMQ::Protocol::Frame::Queue::DeclareOk) || raise "Unexpected frame"
      { queue_name: f.queue_name, message_count: f.message_count, consumer_count: f.consumer_count }
    end

    def publish(bytes : Bytes, exchange, routing_key, opts = {} of String => String)
      publish(IO::Memory.new(bytes), exchange, routing_key, opts)
    end

    def publish(str : String, exchange, routing_key, opts = {} of String => String)
      publish(IO::Memory.new(str), exchange, routing_key, opts)
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
        body_io = IO::Memory.new(header.body_size)
        until body_io.pos == header.body_size
          body = next_frame.as?(AMQ::Protocol::Frame::Body) || raise "Unexpected frame"
          IO.copy(body.body, body_io, body.body_size)
        end
        body_io.rewind
        Message.new(f.exchange, f.routing_key, f.delivery_tag, header.properties, body_io)
      else
        raise "Unexpected frame"
      end
    end

    @consumers = Hash(String, Proc(Message, Nil)).new

    def consume(queue, no_ack = true, exclusive = false, no_wait = false,
                arguments = Hash(String, AMQ::Protocol::Field).new, &blk : Message -> _)
      @connection.write AMQ::Protocol::Frame::Basic::Consume.new(@id, 0_u16, queue, "", false, no_ack, exclusive, false, arguments)
      ok = next_frame.as?(AMQ::Protocol::Frame::Basic::ConsumeOk) || raise "Unexpected frame"
      @consumers[ok.consumer_tag] = blk
      ok
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
