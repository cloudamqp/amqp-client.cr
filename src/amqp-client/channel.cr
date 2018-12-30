require "./connection"
require "./message"
require "./queue"
require "./exchange"

class AMQP::Client
  class UnexpectedFrame < Exception; end

  class Channel
    getter id

    @confirm_mode = false
    @confirm_id = 0_u64
    @publish_nacked = false
    @unconfirmed_set = Set(UInt64).new
    @incoming = ::Channel(AMQ::Protocol::Frame).new
    @delivery = ::Channel(AMQ::Protocol::Frame::Basic::Deliver).new
    @confirms = ::Channel(AMQ::Protocol::Frame).new
    @returns = ::Channel(AMQ::Protocol::Frame::Basic::Return).new
    @log : Logger

    def initialize(@connection : Connection, @id : UInt16)
      @log = @connection.log
      spawn delivery_loop, name: "Channel #{@id} delivery_loop"
      spawn return_loop, name: "Channel #{@id} return_loop"
    end

    def open
      write AMQ::Protocol::Frame::Channel::Open.new(@id)
      next_frame.as?(AMQ::Protocol::Frame::Channel::OpenOk) || raise UnexpectedFrame.new
      self
    end

    def close
      write AMQ::Protocol::Frame::Channel::Close.new(@id)
      next_frame.as?(AMQ::Protocol::Frame::Channel::CloseOk) || raise UnexpectedFrame.new
      cleanup
    end

    def cleanup
      @incoming.close
      @delivery.close
      @confirms.close
    end

    def incoming(frame)
      case frame
      when AMQ::Protocol::Frame::Basic::Deliver
        @delivery.send frame
      when AMQ::Protocol::Frame::Basic::Ack,
           AMQ::Protocol::Frame::Basic::Nack
        @confirms.send frame
      when AMQ::Protocol::Frame::Basic::Return
        @returns.send frame
      else
        @incoming.send frame
      end
    end

    @on_return : Proc(Message, Nil)?
    def on_return(&blk : Message -> _)
      @on_return = blk
    end

    private def return_loop
      loop do
        f = @returns.receive
        header = next_frame.as?(AMQ::Protocol::Frame::Header) || raise UnexpectedFrame.new
        body_io = IO::Memory.new(header.body_size)
        until body_io.pos == header.body_size
          body = next_frame.as?(AMQ::Protocol::Frame::Body) || raise UnexpectedFrame.new
          IO.copy(body.body, body_io, body.body_size)
        end
        body_io.rewind
        msg = Message.new(f.exchange, f.routing_key,
                          header.properties, body_io)
        unless @on_return
          @log.error("Message returned but no on_return block defined: #{msg.inspect}")
          return
        end

        begin
          @on_return.try &.call(msg)
        rescue ex
          @log.error("Uncaught exception in on_return: #{ex.inspect_with_backtrace}")
        end
      end
    end

    def confirm_or_die(timeout = 5)
      raise "Confirms not enabled" unless @confirm_mode
      return if @unconfirmed_set.empty?
      timeout_channel = timeout_channel(timeout)
      loop do
        idx, _ = ::Channel.receive_first(@unconfirmed_set_updated, timeout_channel)
        case idx
        when 0
          raise "Nacked publish" if @publish_nacked
          return if @unconfirmed_set.empty?
        when 1
          raise "Timed out waiting for confirm"
        end
      end
    end

    private def timeout_channel(timeout)
      time_channel = ::Channel(Nil).new
      spawn do
        sleep timeout
        time_channel.send nil
      end
      return time_channel
    end

    @unconfirmed_set_updated = ::Channel(Nil).new

    private def confirm_loop
      loop do
        f = @confirms.receive
        case f
        when AMQ::Protocol::Frame::Basic::Ack
          @unconfirmed_set.delete f.delivery_tag
        when AMQ::Protocol::Frame::Basic::Nack
          @publish_nacked = true
        end
        @unconfirmed_set_updated.send nil
      rescue ::Channel::ClosedError
        break
      end
    end

    private def delivery_loop
      loop do
        f = @delivery.receive
        msg = get_message(f)
        consumer = @consumers[f.consumer_tag]
        begin
          consumer.call(msg)
        rescue ex
          @log.error("Uncaught exception in worker fiber: #{ex.inspect_with_backtrace}")
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    private def next_frame : AMQ::Protocol::Frame
      f = @incoming.receive
      case f
      when AMQ::Protocol::Frame::Channel::Close
        write AMQ::Protocol::Frame::Channel::CloseOk.new(@id)
        cleanup
        raise ClosedException.new(f)
      else
        return f
      end
    end

    def basic_publish(bytes : Bytes, exchange, routing_key, opts = {} of String => AMQ::Protocol::Field)
      basic_publish(IO::Memory.new(bytes), exchange, routing_key, opts)
    end

    def basic_publish(str : String, exchange, routing_key, opts = {} of String => AMQ::Protocol::Field)
      basic_publish(IO::Memory.new(str), exchange, routing_key, opts)
    end

    def basic_publish(io : IO, exchange : String, routing_key : String, opts = {} of String => AMQ::Protocol::Field) : Nil
      write AMQ::Protocol::Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, false, false), flush: false
      write AMQ::Protocol::Frame::Header.new(@id, 60_u16, 0_u16, io.bytesize.to_u64, AMQ::Protocol::Properties.new), flush: io.bytesize.zero?
      until io.pos == io.bytesize
        length = Math.min(@connection.frame_max, io.bytesize.to_u32 - io.pos)
        write AMQ::Protocol::Frame::Body.new(@id, length, io)
      end
      @unconfirmed_set.add(@confirm_id += 1_u64) if @confirm_mode
    end

    def basic_get(queue : String, no_ack : Bool) : DeliveredMessage?
      write AMQ::Protocol::Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      f = next_frame
      case f
      when AMQ::Protocol::Frame::Basic::GetEmpty
        return nil
      when AMQ::Protocol::Frame::Basic::GetOk
        get_message(f.as(AMQ::Protocol::Frame::Basic::GetOk))
      else
        raise UnexpectedFrame.new
      end
    end

    private def get_message(f) : DeliveredMessage
      header = next_frame.as?(AMQ::Protocol::Frame::Header) || raise UnexpectedFrame.new
      body_io = IO::Memory.new(header.body_size)
      until body_io.pos == header.body_size
        body = next_frame.as?(AMQ::Protocol::Frame::Body) || raise UnexpectedFrame.new
        IO.copy(body.body, body_io, body.body_size)
      end
      body_io.rewind
      delivery_tag = case f
                     when AMQ::Protocol::Frame::Basic::Return
                       0_u64
                     else
                       f.delivery_tag
                     end
      DeliveredMessage.new(self, f.exchange, f.routing_key,
                           delivery_tag, header.properties, body_io)
    end

    @consumers = Hash(String, Proc(DeliveredMessage, Nil)).new

    def basic_consume(queue, no_ack = true, exclusive = false,
                arguments = Hash(String, AMQ::Protocol::Field).new, &blk : DeliveredMessage -> _)
      write AMQ::Protocol::Frame::Basic::Consume.new(@id, 0_u16, queue, "", false, no_ack, exclusive, false, arguments)
      ok = next_frame.as?(AMQ::Protocol::Frame::Basic::ConsumeOk) || raise UnexpectedFrame.new
      @consumers[ok.consumer_tag] = blk
      ok.consumer_tag
    end

    def basic_ack(delivery_tag : UInt64, multiple = false) : Nil
      write AMQ::Protocol::Frame::Basic::Ack.new(@id, delivery_tag, multiple)
    end

    def basic_reject(delivery_tag : UInt64, requeue = false) : Nil
      write AMQ::Protocol::Frame::Basic::Reject.new(@id, delivery_tag, requeue)
    end

    def basic_nack(delivery_tag : UInt64, requeue = false, multiple = false) : Nil
      write AMQ::Protocol::Frame::Basic::Reject.new(@id, delivery_tag, multiple, requeue)
    end

    def prefetch(count, global = false) : Nil
      write AMQ::Protocol::Frame::Basic::Qos.new(@id, 0_u32, count, global)
      next_frame.as?(AMQ::Protocol::Frame::Basic::QosOk) || raise UnexpectedFrame.new
    end

    # Declares a temporary queue, which won't be durable and auto-deleted when not used anymore
    def queue
      q = queue_declare("", durable: false, auto_delete: true, exclusive: true)
      Queue.new(self, q[:queue_name])
    end

    # Declares a queue with a name, by default durable and not auto-deleted
    def queue(name : String, passive = false, durable = true, auto_delete = false, exclusive = false, args = Hash(String, AMQ::Protocol::Field).new)
      q = queue_declare(name, passive, durable, auto_delete, exclusive, args)
      Queue.new(self, q[:queue_name])
    end

    def queue_declare(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, args = Hash(String, AMQ::Protocol::Field).new)
      no_wait = false
      write AMQ::Protocol::Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable, exclusive, auto_delete, no_wait, args)
      f = next_frame.as?(AMQ::Protocol::Frame::Queue::DeclareOk) || raise UnexpectedFrame.new
      { queue_name: f.queue_name, message_count: f.message_count, consumer_count: f.consumer_count }
    end

    def queue_delete(name : String, if_unused = false, if_empty = false)
      write AMQ::Protocol::Frame::Queue::Delete.new(@id, 0_u16, name, if_unused, if_empty, no_wait: false)
      f = next_frame.as?(AMQ::Protocol::Frame::Queue::DeleteOk) || raise UnexpectedFrame.new
      { message_count: f.message_count }
    end

    def queue_purge(name : String)
      write AMQ::Protocol::Frame::Queue::Purge.new(@id, 0_u16, name, no_wait: false)
      f = next_frame.as?(AMQ::Protocol::Frame::Queue::PurgeOk) || raise UnexpectedFrame.new
      { message_count: f.message_count }
    end

    def queue_bind(queue : String, exchange : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new) : Nil
      write AMQ::Protocol::Frame::Queue::Bind.new(@id, 0_u16, queue, exchange, routing_key, no_wait, args)
      next_frame.as?(AMQ::Protocol::Frame::Queue::BindOk) || raise UnexpectedFrame.new unless no_wait
    end

    def queue_unbind(queue : String, exchange : String, routing_key : String, args = Hash(String, AMQ::Protocol::Field).new) : Nil
      write AMQ::Protocol::Frame::Queue::Bind.new(@id, 0_u16, queue, exchange, routing_key, args)
      next_frame.as?(AMQ::Protocol::Frame::Queue::BindOk) || raise UnexpectedFrame.new
    end

    def topic_exchange(name = "amq.topic", passive = true)
      exchange(name, "topic", passive)
    end

    def fanout_exchange(name = "amq.fanout", passive = true)
      exchange(name, "fanout", passive)
    end

    def header_exchange(name = "amq.header", passive = true)
      exchange(name, "header", passive)
    end

    def direct_exchange(name = "amq.direct", passive = true)
      exchange(name, "direct", passive)
    end

    def default_exchange
      exchange("", "direct", true)
    end

    def exchange(name, type, passive = false)
      exchange_declare(name, type, passive)
      Exchange.new(self, name)
    end

    def exchange_declare(name : String, type : String, passive = false, durable = true,
                         exclusive = false, internal = false, auto_delete = false,
                         no_wait = false, args = Hash(String, AMQ::Protocol::Field).new) : Nil
      write AMQ::Protocol::Frame::Exchange::Declare.new(@id, 0_u16, name, type, passive, durable, auto_delete, internal, no_wait, args)
      next_frame.as?(AMQ::Protocol::Frame::Exchange::DeclareOk) || raise UnexpectedFrame.new unless no_wait
    end

    def exchange_delete(name, if_unused = false, no_wait = false)
      write AMQ::Protocol::Frame::Exchange::Delete.new(@id, 0_u16, name, if_unused, no_wait)
      next_frame.as?(AMQ::Protocol::Frame::Exchange::DeleteOk) || raise UnexpectedFrame.new unless no_wait
    end

    def exchange_bind(source : String, destination : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new) : Nil
      write AMQ::Protocol::Frame::Exchange::Bind.new(@id, 0_u16, source, destination, routing_key, no_wait, args)
      next_frame.as?(AMQ::Protocol::Frame::Queue::BindOk) || raise UnexpectedFrame.new unless no_wait
    end

    def exchange_unbind(source : String, destination : String, routing_key : String, no_wait = false, args = Hash(String, AMQ::Protocol::Field).new) : Nil
      write AMQ::Protocol::Frame::Exchange::Unbind.new(@id, 0_u16, source, destination, routing_key, no_wait, args)
      next_frame.as?(AMQ::Protocol::Frame::Queue::UnbindOk) || raise UnexpectedFrame.new unless no_wait
    end

    def confirm_select : Nil
      return if @confirm_mode
      write AMQ::Protocol::Frame::Confirm::Select.new(@id, no_wait = false)
      next_frame.as?(AMQ::Protocol::Frame::Confirm::SelectOk) || raise UnexpectedFrame.new unless no_wait
      spawn confirm_loop, name: "Channel #{@id} confirm_loop"
      @confirm_mode = true
    end

    private def write(frame, flush = true)
      @connection.write frame, flush
    end

    class ClosedException < Exception
      def initialize(close : AMQ::Protocol::Frame::Channel::Close)
        super(close.reply_text)
      end
    end
  end
end
