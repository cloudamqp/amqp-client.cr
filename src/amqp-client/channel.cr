require "./connection"
require "./message"
require "./queue"
require "./exchange"

class AMQP::Client
  alias Frame = AMQ::Protocol::Frame
  alias Arguments = Hash(String, AMQ::Protocol::Field)
  alias Properties = AMQ::Protocol::Properties
  class UnexpectedFrame < Exception
    def initialize
      super
    end
    def initialize(frame : Frame)
      super(frame.inspect)
    end
  end

  class Channel
    getter id

    @confirm_mode = false
    @confirm_id = 0_u64
    @incoming = ::Channel(Frame).new
    @frames = ::Channel(Frame).new
    @confirms = ::Channel(Frame::Basic::Ack | Frame::Basic::Nack).new(1024)
    @log : Logger

    def initialize(@connection : Connection, @id : UInt16)
      @log = @connection.log
      spawn read_loop, name: "Channel #{@id} read_loop"
    end

    def open
      write Frame::Channel::Open.new(@id)
      expect Frame::Channel::OpenOk
      self
    end

    @on_close : Proc((UInt16, String), Nil)? 

    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    def close : Bool
      return false if @closed
      write Frame::Channel::Close.new(@id, 320, "Bye", 0, 0)
      expect Frame::Channel::CloseOk
      cleanup
    end

    def close(frame : Frame::Channel::Close) : Bool
      @closing_frame = frame
      @log.info "Channel #{@id} closed by server: #{frame.inspect}" unless @on_close
      begin
        @on_close.try &.call(frame.reply_code, frame.reply_text)
      rescue ex
        @log.error "Uncaught exception in on_close block: #{ex.inspect_with_backtrace}"
      end
      write Frame::Channel::CloseOk.new(@id)
      cleanup
    end

    def closed?
      @closed
    end

    def cleanup
      @confirms.close
      @frames.close
      @incoming.close
      @closed = true
    end

    def incoming(frame)
      @incoming.send frame
    end

    private def read_loop
      loop do
        frame = @incoming.receive
        case frame
        when Frame::Channel::Close then close(frame)
        when Frame::Basic::Deliver then process_deliver(frame)
        when Frame::Basic::Return then process_return(frame)
        when Frame::Basic::Cancel then process_cancel(frame)
        when Frame::Basic::Ack, Frame::Basic::Nack
          @confirms.send frame
        else
          @frames.send frame
        end
      end
    rescue ::Channel::ClosedError
    end

    @on_cancel : Proc(String, Nil)?

    def on_cancel(&blk : String -> Nil)
      @on_cancel = blk
    end

    private def process_cancel(f : Frame::Basic::Cancel)
      @log.warn("Consumer #{f.consumer_tag} canceled by server") unless @on_cancel

      begin
        @on_cancel.try &.call(f.consumer_tag)
      rescue ex
        @log.error("Uncaught exception in on_return: #{ex.inspect_with_backtrace}")
      end

      write Frame::Basic::CancelOk.new(@id, f.consumer_tag) unless f.no_wait
      @consumers.delete(f.consumer_tag)
    end

    private def process_deliver(f : Frame::Basic::Deliver)
      header = @incoming.receive.as(Frame::Header)
      body_io = IO::Memory.new(header.body_size)
      until body_io.pos == header.body_size
        body = @incoming.receive.as(Frame::Body)
        IO.copy(body.body, body_io, body.body_size)
      end
      body_io.rewind
      msg = Message.new(self, f.exchange, f.routing_key,
                        f.delivery_tag, header.properties,
                        body_io, f.redelivered)
      consumer = @consumers[f.consumer_tag]
      begin
        consumer.call(msg)
      rescue ex
        @log.error("Uncaught exception in consumer: #{ex.inspect_with_backtrace}")
      end
    end

    @on_return : Proc(ReturnedMessage, Nil)?

    def on_return(&blk : ReturnedMessage -> Nil)
      @on_return = blk
    end

    private def process_return(return_frame)
      header = @incoming.receive.as(Frame::Header)
      body_io = IO::Memory.new(header.body_size)
      until body_io.pos == header.body_size
        body = @incoming.receive.as(Frame::Body)
        IO.copy(body.body, body_io, body.body_size)
      end
      body_io.rewind
      msg = ReturnedMessage.new(return_frame.reply_code,
                                return_frame.reply_text,
                                return_frame.exchange,
                                return_frame.routing_key,
                                header.properties, body_io)
      unless @on_return
        @log.error("Message returned but no on_return block defined: #{msg.inspect}")
        return
      end

      begin
        @on_return.try &.call(msg.not_nil!)
      rescue ex
        @log.error("Uncaught exception in on_return: #{ex.inspect_with_backtrace}")
      end
    end

    def basic_publish(bytes : Bytes, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new)
      basic_publish(IO::Memory.new(bytes), exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(str : String, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new)
      basic_publish(IO::Memory.new(str), exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(io : IO, exchange : String, routing_key : String,
                      mandatory = false, immediate = false, props = Properties.new) : UInt64?
      write Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, mandatory, immediate), flush: false
      write Frame::Header.new(@id, 60_u16, 0_u16, io.bytesize.to_u64, props), flush: io.bytesize.zero?
      until io.pos == io.bytesize
        length = Math.min(@connection.frame_max, io.bytesize.to_u32 - io.pos)
        write Frame::Body.new(@id, length, io), flush: true
      end
      @confirm_id += 1_u64 if @confirm_mode
    end

    def basic_publish_confirm(msg, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(msg, exchange, routing_key, mandatory, immediate, props).not_nil!
      loop do
        confirm = @confirms.receive
        case confirm
        when Frame::Basic::Ack
          next if confirm.delivery_tag < msgid
          next if confirm.delivery_tag > msgid && !confirm.multiple
          return true
        when Frame::Basic::Nack
          next if confirm.delivery_tag < msgid
          next if confirm.delivery_tag > msgid && !confirm.multiple
          return false
        else raise UnexpectedFrame.new(confirm)
        end
      end
    end

    def basic_get(queue : String, no_ack : Bool) : Message?
      write Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      f = next_frame
      case f
      when Frame::Basic::GetOk then get_message(f)
      when Frame::Basic::GetEmpty then nil
      else raise UnexpectedFrame.new(f)
      end
    end

    private def get_message(f) : Message
      header = expect Frame::Header
      body_io = IO::Memory.new(header.body_size)
      until body_io.pos == header.body_size
        body = expect Frame::Body
        IO.copy(body.body, body_io, body.body_size)
      end
      body_io.rewind
      Message.new(self, f.exchange, f.routing_key,
                  f.delivery_tag, header.properties, body_io,
                  f.redelivered)
    end

    def has_subscriber?(consumer_tag)
      @consumers.has_key? consumer_tag
    end

    @consumers = Hash(String, Proc(Message, Nil)).new

    def basic_consume(queue, tag = "", no_ack = true, exclusive = false,
                      args = Arguments.new, &blk : Message -> Nil)
      write Frame::Basic::Consume.new(@id, 0_u16, queue, tag, false, no_ack, exclusive, false, args)
      ok = expect Frame::Basic::ConsumeOk
      @consumers[ok.consumer_tag] = blk
      ok.consumer_tag
    end

    def basic_cancel(consumer_tag, no_wait = false) : Nil
      write Frame::Basic::Cancel.new(@id, consumer_tag, no_wait)
      expect Frame::Basic::CancelOk unless no_wait
      @consumers.delete(consumer_tag)
    end

    def basic_ack(delivery_tag : UInt64, multiple = false) : Nil
      write Frame::Basic::Ack.new(@id, delivery_tag, multiple)
    end

    def basic_reject(delivery_tag : UInt64, requeue = false) : Nil
      write Frame::Basic::Reject.new(@id, delivery_tag, requeue)
    end

    def basic_nack(delivery_tag : UInt64, requeue = false, multiple = false) : Nil
      write Frame::Basic::Reject.new(@id, delivery_tag, multiple, requeue)
    end

    def basic_qos(count, global = false) : Nil
      write Frame::Basic::Qos.new(@id, 0_u32, count.to_u16, global)
      expect Frame::Basic::QosOk
    end

    def prefetch(count, global = false) : Nil
      basic_qos(count, global)
    end

    # Declares a temporary queue, which won't be durable and auto-deleted when not used anymore
    def queue
      q = queue_declare("", durable: false, auto_delete: true, exclusive: true)
      Queue.new(self, q[:queue_name])
    end

    # Declares a queue with a name, by default durable and not auto-deleted
    def queue(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, args = Arguments.new)
      q = queue_declare(name, passive, durable, exclusive, auto_delete, args)
      Queue.new(self, q[:queue_name])
    end

    def queue_declare(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, args = Arguments.new)
      no_wait = false
      write Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable,
                                      exclusive, auto_delete, no_wait,
                                      args)
      f = expect Frame::Queue::DeclareOk
      {
        queue_name: f.queue_name,
        message_count: f.message_count,
        consumer_count: f.consumer_count
      }
    end

    def queue_delete(name : String, if_unused = false, if_empty = false)
      write Frame::Queue::Delete.new(@id, 0_u16, name, if_unused, if_empty, no_wait: false)
      f = expect Frame::Queue::DeleteOk
      { message_count: f.message_count }
    end

    def queue_purge(name : String)
      write Frame::Queue::Purge.new(@id, 0_u16, name, no_wait: false)
      f = expect Frame::Queue::PurgeOk
      { message_count: f.message_count }
    end

    def queue_bind(queue : String, exchange : String, routing_key : String, no_wait = false, args = Arguments.new) : Nil
      write Frame::Queue::Bind.new(@id, 0_u16, queue, exchange, routing_key, no_wait, args)
      expect Frame::Queue::BindOk unless no_wait
    end

    def queue_unbind(queue : String, exchange : String, routing_key : String, args = Arguments.new) : Nil
      write Frame::Queue::Unbind.new(@id, 0_u16, queue, exchange, routing_key, args)
      expect Frame::Queue::UnbindOk
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
      Exchange.new(self, "")
    end

    def exchange(name, type, passive = false, durable = true, exclusive = false,
                 internal = false, auto_delete = false, args = Arguments.new)
      exchange_declare(name, type, passive, durable, exclusive, internal, auto_delete, false, args)
      Exchange.new(self, name)
    end

    def exchange_declare(name : String, type : String, passive = false,
                         durable = true, exclusive = false,
                         internal = false, auto_delete = false,
                         no_wait = false, args = Arguments.new) : Nil
      write Frame::Exchange::Declare.new(@id, 0_u16, name, type, passive,
                                         durable, auto_delete, internal,
                                         no_wait, args)
      expect Frame::Exchange::DeclareOk unless no_wait
    end

    def exchange_delete(name, if_unused = false, no_wait = false) : Nil
      write Frame::Exchange::Delete.new(@id, 0_u16, name, if_unused, no_wait)
      expect Frame::Exchange::DeleteOk unless no_wait
    end

    def exchange_bind(source : String, destination : String, routing_key : String, no_wait = false, args = Arguments.new) : Nil
      write Frame::Exchange::Bind.new(@id, 0_u16, source, destination, routing_key, no_wait, args)
      expect Frame::Exchange::BindOk unless no_wait
    end

    def exchange_unbind(source : String, destination : String, routing_key : String, no_wait = false, args = Arguments.new) : Nil
      write Frame::Exchange::Unbind.new(@id, 0_u16, source, destination, routing_key, no_wait, args)
      expect Frame::Queue::UnbindOk unless no_wait
    end

    def confirm_select(no_wait = false) : Nil
      return if @confirm_mode
      write Frame::Confirm::Select.new(@id, no_wait)
      expect Frame::Confirm::SelectOk unless no_wait
      @confirm_mode = true
    end

    def basic_recover(requeue) : Nil
      write Frame::Basic::Recover.new(@id, requeue)
      expect Frame::Basic::RecoverOk
    end

    private def write(frame, flush = true)
      @connection.write frame, flush
    end

    private def next_frame : Frame
      @frames.receive
    rescue ex : ::Channel::ClosedError
      raise ClosedException.new(@closing_frame.not_nil!) if @closing_frame
      raise ex
    end

    macro expect(clz)
      frame = next_frame
      frame.as?({{ clz }}) || raise UnexpectedFrame.new(frame)
    end

    class ClosedException < Exception
      def initialize(close : Frame::Channel::Close)
        super("#{close.reply_code} - #{close.reply_text}")
      end
    end
  end
end
