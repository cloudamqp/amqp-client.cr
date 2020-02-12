require "./connection"
require "./message"
require "./queue"
require "./exchange"

class AMQP::Client
  class Channel
    getter id

    @closed = false
    @log : Logger
    @confirm_mode = false
    @confirm_id = 0_u64
    @server_flow = true

    @server_frames = ::Channel(Frame).new
    @reply_frames = ::Channel(Frame).new
    @next_msg_ready = ::Channel(Nil).new

    @deliveries = ::Deque(Tuple(Frame::Basic::Deliver, Properties, IO::Memory)).new(128)
    @has_delivery = ::Channel(Nil).new(1)
    @returns = ::Deque(Tuple(Frame::Basic::Return, Properties, IO::Memory)).new(128)
    @has_return = ::Channel(Nil).new(1)
    @confirms = ::Deque(Frame::Basic::Ack | Frame::Basic::Nack).new(128)
    @has_confirms = ::Channel(Nil).new(1)

    def initialize(@connection : Connection, @id : UInt16)
      @log = @connection.log
      spawn read_loop, name: "Channel #{@id} read_loop"
      spawn delivery_loop, name: "Channel #{@id} delivery_loop"
      spawn return_loop, name: "Channel #{@id} return_loop"
    end

    def open
      write Frame::Channel::Open.new(@id)
      expect Frame::Channel::OpenOk
      self
    end

    @on_close : Proc(UInt16, String, Nil)?

    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    def close : Nil
      return if @closed
      @closed = true
      write Frame::Channel::Close.new(@id, 200, "", 0, 0)
      expect Frame::Channel::CloseOk
      cleanup
    end

    def close(frame : Frame::Channel::Close) : Nil
      @closed = true
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

    def flow(active : Bool)
      write Frame::Channel::Flow.new(@id, active)
    end

    def closed?
      @closed
    end

    def cleanup
      @has_delivery.close
      @has_return.close
      @has_confirms.close
      @reply_frames.close
      @server_frames.close
    end

    @next_body_io = IO::Memory.new(0)
    @next_body_size = 0_u32
    @next_msg_props = AMQ::Protocol::Properties.new

    def incoming(frame)
      case frame
      when Frame::Channel::Close,
           Frame::Channel::Flow,
           Frame::Basic::Deliver,
           Frame::Basic::Return,
           Frame::Basic::Cancel
        @server_frames.send frame
      when Frame::Basic::Ack, Frame::Basic::Nack
        was_empty = @confirms.empty?
        @confirms.push frame
        @has_confirms.send nil if was_empty
      when Frame::Header
        @next_msg_props = frame.properties
        @next_body_size = frame.body_size.to_u32
        @next_body_io = IO::Memory.new(frame.body_size)
        @next_msg_ready.send nil if frame.body_size.zero?
      when Frame::Body
        IO.copy(frame.body, @next_body_io, frame.body_size)
        if @next_body_io.pos == @next_body_size
          @next_body_io.rewind
          @next_msg_ready.send nil
        end
      else
        @reply_frames.send frame
      end
    end

    private def read_loop
      loop do
        frame = @server_frames.receive
        case frame
        when Frame::Channel::Close then close(frame)
        when Frame::Channel::Flow  then process_flow(frame.active)
        when Frame::Basic::Deliver then process_deliver(frame)
        when Frame::Basic::Return  then process_return(frame)
        when Frame::Basic::Cancel  then process_cancel(frame)
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    @on_cancel : Proc(String, Nil)?

    def on_cancel(&blk : String -> Nil)
      @on_cancel = blk
    end

    private def process_cancel(f : Frame::Basic::Cancel)
      @log.warn("Consumer #{f.consumer_tag} cancelled by server") unless @on_cancel
      begin
        @on_cancel.try &.call(f.consumer_tag)
      rescue ex
        @log.error("Uncaught exception in on_cancel: #{ex.inspect_with_backtrace}")
      end

      if cb = @consumer_blocks.delete f.consumer_tag
        cb.close
      end
      write Frame::Basic::CancelOk.new(@id, f.consumer_tag) unless f.no_wait
      @consumers.delete(f.consumer_tag)
    end

    private def process_deliver(f : Frame::Basic::Deliver)
      @next_msg_ready.receive
      was_empty = @deliveries.empty?
      @deliveries.push({ f, @next_msg_props, @next_body_io })
      @has_delivery.send nil if was_empty
    end

    private def delivery_loop
      loop do
        @has_delivery.receive
        while d = @deliveries.shift?
          f, props, body_io = d
          msg = DeliverMessage.new(self, f.exchange, f.routing_key,
            f.delivery_tag, props, body_io, f.redelivered)
          if consumer = @consumers.fetch(f.consumer_tag, nil)
            begin
              consumer.call(msg)
            rescue ex
              if cb = @consumer_blocks.delete f.consumer_tag
                cb.send ex
              else
                @log.error("Uncaught exception in consumer: #{ex.inspect_with_backtrace}")
              end
            end
          else
            @log.warn("No consumer #{f.consumer_tag} found")
          end
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    @on_return : Proc(ReturnedMessage, Nil)?

    def on_return(&blk : ReturnedMessage -> Nil)
      @on_return = blk
    end

    private def process_return(return_frame)
      @next_msg_ready.receive
      was_empty = @returns.empty?
      @returns.push({ return_frame, @next_msg_props, @next_body_io })
      @has_return.send nil if was_empty
    end

    private def return_loop
      loop do
        @has_return.receive
        while r = @returns.shift?
          f, props, body_io = r
          msg = ReturnedMessage.new(f.reply_code, f.reply_text,
                                    f.exchange, f.routing_key,
                                    props, body_io)
          if @on_return
            begin
              @on_return.try &.call(msg)
            rescue ex
              @log.error("Uncaught exception in on_return: #{ex.inspect_with_backtrace}")
            end
          else
            @log.error("Message returned but no on_return block defined: #{msg.inspect}")
          end
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    private def process_flow(active : Bool)
      @server_flow = active
      write Frame::Channel::FlowOk.new(@id, active)
    end

    def basic_publish(bytes : Bytes, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new)
      basic_publish(IO::Memory.new(bytes), exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(str : String, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new)
      basic_publish(IO::Memory.new(str), exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(io : (IO::Memory | IO::FileDescriptor), exchange, routing_key, mandatory = false, immediate = false, props = Properties.new)
      basic_publish(io, io.bytesize, exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(io : IO, bytesize : Int, exchange : String, routing_key : String,
                      mandatory = false, immediate = false, props = Properties.new) : UInt64
      @connection.write Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, mandatory, immediate), flush: false
      @connection.write Frame::Header.new(@id, 60_u16, 0_u16, bytesize.to_u64, props), flush: false
      pos = 0_u32
      until pos == bytesize
        length = Math.min(@connection.frame_max, bytesize.to_u32 - pos)
        @connection.write Frame::Body.new(@id, length, io), flush: false
        pos += length
      end
      @connection.flush
      if @confirm_mode
        @confirm_id += 1_u64
      else
        0_u64
      end
    end

    def basic_publish_confirm(msg, exchange, routing_key, mandatory = false, immediate = false, props = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(msg, exchange, routing_key, mandatory, immediate, props)
      wait_for_confirm(msgid)
    end

    def basic_publish_confirm(io : IO, bytesize : Int, exchange : String, routing_key : String, mandatory = false, immediate = false, props = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(io, bytesize, exchange, routing_key, mandatory, immediate, props)
      wait_for_confirm(msgid)
    end


    def wait_for_confirm(msgid) : Bool
      raise ArgumentError.new "Confirm id must be > 0" unless msgid > 0
      loop do
        @has_confirms.receive
        while confirm = @confirms.shift?
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
      rescue ex : ::Channel::ClosedError
        raise ClosedException.new(@closing_frame, cause: ex)
      end
    end

    def basic_get(queue : String, no_ack : Bool) : GetMessage?
      write Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      f = next_frame
      case f
      when Frame::Basic::GetOk    then get_message(f)
      when Frame::Basic::GetEmpty then nil
      else                             raise UnexpectedFrame.new(f)
      end
    end

    private def get_message(f) : GetMessage
      @next_msg_ready.receive
      GetMessage.new(self, f.exchange, f.routing_key,
                  f.delivery_tag, @next_msg_props, @next_body_io,
                  f.redelivered, f.message_count)
    end

    def has_subscriber?(consumer_tag)
      @consumers.has_key? consumer_tag
    end

    @consumers = Hash(String, Proc(DeliverMessage, Nil)).new
    @consumer_blocks = Hash(String, ::Channel(Exception)).new

    def basic_consume(queue, tag = "", no_ack = true, exclusive = false,
                      block = false,
                      args = Arguments.new, &blk : DeliverMessage -> Nil)
      write Frame::Basic::Consume.new(@id, 0_u16, queue, tag, false, no_ack, exclusive, false, args)
      ok = expect Frame::Basic::ConsumeOk
      @consumers[ok.consumer_tag] = blk
      if block
        cb = @consumer_blocks[ok.consumer_tag] = ::Channel(Exception).new
        if ex = cb.receive?
          @consumers.delete(ok.consumer_tag)
          write Frame::Basic::Cancel.new(@id, ok.consumer_tag, no_wait: true)
          raise ex
        end
      end
      ok.consumer_tag
    end

    def basic_cancel(consumer_tag, no_wait = false) : Nil
      write Frame::Basic::Cancel.new(@id, consumer_tag, no_wait)
      expect Frame::Basic::CancelOk unless no_wait
      if cb = @consumer_blocks.delete consumer_tag
        cb.close
      end
      @consumers.delete(consumer_tag)
    end

    def basic_ack(delivery_tag : UInt64, multiple = false) : Nil
      write Frame::Basic::Ack.new(@id, delivery_tag, multiple)
    end

    def basic_reject(delivery_tag : UInt64, requeue = false) : Nil
      write Frame::Basic::Reject.new(@id, delivery_tag, requeue)
    end

    def basic_nack(delivery_tag : UInt64, requeue = false, multiple = false) : Nil
      write Frame::Basic::Nack.new(@id, delivery_tag, multiple, requeue)
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
      durable = false if name.empty?
      exclusive = true if name.empty?
      auto_delete = true if name.empty?
      no_wait = false
      write Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable,
                                      exclusive, auto_delete, no_wait,
                                      args)
      f = expect Frame::Queue::DeclareOk
      {
        queue_name:     f.queue_name,
        message_count:  f.message_count,
        consumer_count: f.consumer_count,
      }
    end

    def queue_delete(name : String, if_unused = false, if_empty = false)
      write Frame::Queue::Delete.new(@id, 0_u16, name, if_unused, if_empty, no_wait: false)
      f = expect Frame::Queue::DeleteOk
      {message_count: f.message_count}
    end

    def queue_purge(name : String)
      write Frame::Queue::Purge.new(@id, 0_u16, name, no_wait: false)
      f = expect Frame::Queue::PurgeOk
      {message_count: f.message_count}
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

    private def write(frame)
      @connection.write frame
    end

    private def next_frame : Frame
      @reply_frames.receive
    rescue ex : ::Channel::ClosedError
      raise ClosedException.new(@closing_frame, cause: ex)
    end

    macro expect(clz)
      @log.debug { "Channel #{@id} expecting {{ clz }}" }
      frame = next_frame
      frame.as?({{ clz }}) || raise UnexpectedFrame.new(frame)
    end

    def inspect(io : IO) : Nil
      io << "#<" << self.class.name << " @id=" << @id << '>'
    end

    class ClosedException < Exception
      def initialize(close : Frame::Channel::Close?, cause = nil)
        if close
          super("#{close.reply_code} - #{close.reply_text}", cause)
        else
          super("Unexpectedly closed channel", cause)
        end
      end
    end
  end
end
