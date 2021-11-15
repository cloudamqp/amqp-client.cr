require "./connection"
require "./message"
require "./queue"
require "./exchange"
require "./sync"

class AMQP::Client
  class Channel
    getter id

    LOG = AMQP::Client::Connection::LOG.for(self)

    @closed = false
    @confirm_mode = false
    @confirm_id = 0_u64
    @server_flow = true

    @reply_frames = ::Channel(Frame).new
    @basic_get = ::Channel(GetMessage?).new
    @confirms = ::Channel(Frame::Basic::Ack | Frame::Basic::Nack).new(8192)

    def initialize(@connection : Connection, @id : UInt16)
      spawn confirm_loop, name: "Channel #{@id} confirm_loop", same_thread: true
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
      begin
        write Frame::Channel::CloseOk.new(@id)
      rescue ex
        LOG.error(exception: ex) { "Couldn't write CloseOk frame" }
      end
      @closed = true
      @closing_frame = frame
      LOG.info { "Channel closed by server: #{frame.inspect}" } unless @on_close
      begin
        @on_close.try &.call(frame.reply_code, frame.reply_text)
      rescue ex
        LOG.error(exception: ex) { "Uncaught exception in on_close block" }
      end
      cleanup
    end

    def flow(active : Bool)
      write Frame::Channel::Flow.new(@id, active)
      expect Frame::Channel::FlowOk
    end

    def closed?
      @closed
    end

    def cleanup
      @closed = true
      @reply_frames.close
      @basic_get.close
      @confirms.close
      @consumer_blocks.each_value(&.close)
      @consumer_blocks.clear
      @consumers.each_value(&.close)
      @consumers.clear
    end

    @next_msg_deliver : Frame::Basic::Deliver | Frame::Basic::Return | Frame::Basic::GetOk | Nil
    @next_msg_props : AMQ::Protocol::Properties?
    @next_body_io : IO::Memory?
    @next_body_size = 0_u32

    def incoming(frame)
      case frame
      when Frame::Basic::Deliver,
           Frame::Basic::Return,
           Frame::Basic::GetOk
        @next_msg_deliver = frame
      when Frame::Basic::GetEmpty
        @basic_get.send(nil)
      when Frame::Header
        @next_msg_props = frame.properties
        @next_body_size = frame.body_size.to_u32
        @next_body_io = IO::Memory.new(frame.body_size)
        process_deliver if frame.body_size.zero?
      when Frame::Body
        body_io = @next_body_io.not_nil!
        IO.copy(frame.body, body_io, frame.body_size)
        if body_io.pos == @next_body_size
          body_io.rewind
          process_deliver
        end
      when Frame::Basic::Cancel
        process_cancel(frame.consumer_tag, frame.no_wait)
      when Frame::Basic::CancelOk
        process_cancel_ok(frame.consumer_tag)
      when Frame::Basic::Ack, Frame::Basic::Nack
        @confirms.send frame
      when Frame::Channel::Flow
        process_flow frame.active
      when Frame::Channel::Close
        close frame
      else
        @reply_frames.send frame
        Fiber.yield
      end
    end

    @on_cancel : Proc(String, Nil)?

    def on_cancel(&blk : String -> Nil)
      @on_cancel = blk
    end

    private def process_cancel(consumer_tag : String, no_wait : Bool)
      LOG.warn { "Consumer #{consumer_tag} cancelled by server" } unless @on_cancel || @closed || @connection.closed?
      begin
        @on_cancel.try &.call(consumer_tag)
      rescue ex
        LOG.error(exception: ex) { "Uncaught exception in on_cancel" }
      end

      process_cancel_ok(consumer_tag)
      write Frame::Basic::CancelOk.new(@id, consumer_tag) unless no_wait
    end

    private def process_cancel_ok(consumer_tag : String)
      if deliveries = @consumers.delete(consumer_tag)
        deliveries.close
      else
        LOG.warn { "Consumer tag '#{consumer_tag}' already cancelled" }
      end
    end

    private def process_deliver
      case f = @next_msg_deliver
      when Frame::Basic::Deliver
        if deliveries = @consumers[f.consumer_tag]?
          msg = DeliverMessage.new(self, f.exchange, f.routing_key,
                                   f.delivery_tag, @next_msg_props.not_nil!, @next_body_io.not_nil!,
                                   f.redelivered)
          deliveries.send(msg)
        else
          LOG.warn { "Consumer tag '#{f.consumer_tag}' not found" }
        end
      when Frame::Basic::Return
        msg = ReturnedMessage.new(f.reply_code, f.reply_text,
                                  f.exchange, f.routing_key, @next_msg_props.not_nil!, @next_body_io.not_nil!)
        if on_return = @on_return
          spawn on_return.call(msg), name: "AMQP::Client::Channel#on_return"
        else
          LOG.error { "Message returned but no on_return block defined: #{msg.inspect}" }
        end
      when Frame::Basic::GetOk
        msg = GetMessage.new(self, f.exchange, f.routing_key,
                             f.delivery_tag, @next_msg_props.not_nil!, @next_body_io.not_nil!,
                             f.redelivered, f.message_count)
        @basic_get.send(msg)
      else
        raise Error.new("BUG: Unexpected process_delivery frame #{f.inspect}")
      end
    ensure
      @next_msg_deliver = nil
      @next_msg_props = nil
      @next_body_io = nil
      @next_body_size = 0_u32
    end

    private def consume(consumer_tag, deliveries, blk)
      LOG.context.set channel_id: @id.to_i, consumer: consumer_tag, fiber: "consumer##{consumer_tag}"
      loop do
        msg = deliveries.receive? || break
        begin
          blk.call(msg)
        rescue ex
          if cb = @consumer_blocks.delete consumer_tag
            cb.send ex
          else
            LOG.error(exception: ex) { "Uncaught exception in consumer" }
          end
        end
      end
      @consumer_blocks.delete(consumer_tag).try &.close
    end

    @on_return : Proc(ReturnedMessage, Nil)?

    def on_return(&blk : ReturnedMessage -> Nil)
      @on_return = blk
    end

    private def process_flow(active : Bool)
      @server_flow = active
      write Frame::Channel::FlowOk.new(@id, active)
    end

    def basic_publish(bytes : Bytes, exchange, routing_key = "", mandatory = false, immediate = false, props = Properties.new)
      basic_publish(bytes, bytes.size, exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(str : String, exchange, routing_key = "", mandatory = false, immediate = false, props = Properties.new)
      basic_publish(str.to_slice, exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(io : (IO::Memory | IO::FileDescriptor), exchange, routing_key = "", mandatory = false, immediate = false, props = Properties.new)
      basic_publish(io, io.bytesize, exchange, routing_key, mandatory, immediate, props)
    end

    def basic_publish(body : IO | Bytes, bytesize : Int, exchange : String, routing_key = "",
                      mandatory = false, immediate = false, props = Properties.new) : UInt64
      raise ClosedException.new(@closing_frame) if @closing_frame

      @connection.with_lock do |c|
        c.unsafe_write Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, mandatory, immediate)
        c.unsafe_write Frame::Header.new(@id, 60_u16, 0_u16, bytesize.to_u64, props)
        pos = 0_u32
        frame_max = @connection.frame_max
        until pos == bytesize
          length = Math.min(frame_max, bytesize.to_u32 - pos)
          case body
          when Bytes
            c.unsafe_write Frame::BytesBody.new(@id, length, body[pos.to_i32, length.to_i32])
          when IO
            c.unsafe_write Frame::Body.new(@id, length, body)
          end
          pos += length
        end
      end
      if @confirm_mode
        @confirm_id += 1_u64
      else
        0_u64
      end
    end

    def basic_publish_confirm(msg, exchange, routing_key = "", mandatory = false, immediate = false, props = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(msg, exchange, routing_key, mandatory, immediate, props)
      wait_for_confirm(msgid)
    end

    def basic_publish_confirm(io : IO, bytesize : Int, exchange : String, routing_key = "", mandatory = false, immediate = false, props = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(io, bytesize, exchange, routing_key, mandatory, immediate, props)
      wait_for_confirm(msgid)
    end

    # Block until confirmed
    def wait_for_confirm(msgid) : Bool
      ch = ::Channel(Bool).new
      on_confirm(msgid) do |acked|
        ch.send(acked)
      end
      ch.receive
    ensure
      raise ClosedException.new(@closing_frame) if @closing_frame
    end

    @on_confirm = Sync(Hash(UInt64, Proc(Bool, Nil))).new
    @last_confirm = {0_u64, true}

    def on_confirm(msgid, &blk : Bool -> Nil)
      raise ArgumentError.new "Confirm id must be > 0" unless msgid > 0
      last_confirm, last_confirm_ok = @last_confirm
      if last_confirm >= msgid.to_u64
        blk.call last_confirm_ok
      else
        @on_confirm[msgid.to_u64] = blk
      end
    end

    private def confirm_loop
      LOG.context.set channel_id: @id.to_i, fiber: "confirm_loop"
      loop do
        confirm = @confirms.receive? || break
        case confirm
        when Frame::Basic::Ack, Frame::Basic::Nack
          acked = confirm.is_a? Frame::Basic::Ack
          @last_confirm = {confirm.delivery_tag, acked}
          if confirm.multiple
            @on_confirm.reject! do |msgid, blk|
              if msgid <= confirm.delivery_tag
                blk.call acked
                true
              else
                false
              end
            end
          elsif blk = @on_confirm.delete confirm.delivery_tag
            blk.call acked
          end
        else raise UnexpectedFrame.new(confirm)
        end
      end
      @on_confirm.reject! do |msgid, blk|
        LOG.debug { "Channel #{@id} hasn't been able to confirm delivery tag #{msgid}" }
        blk.call false
        true
      end
    end

    def basic_get(queue : String, no_ack : Bool) : GetMessage?
      write Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      @basic_get.receive
    end

    def has_subscriber?(consumer_tag)
      @consumers.has_key? consumer_tag
    end

    @consumers = Sync(Hash(String, ::Channel(DeliverMessage))).new
    @consumer_blocks = Sync(Hash(String, ::Channel(Exception))).new

    def basic_consume(queue, tag = "", no_ack = true, exclusive = false,
                      block = false, args = Arguments.new, work_pool = 1,
                      &blk : DeliverMessage -> Nil)
      raise ArgumentError.new("Max allowed work_pool is 1024") if work_pool > 1024
      write Frame::Basic::Consume.new(@id, 0_u16, queue, tag, false, no_ack, exclusive, false, args)
      ok = expect Frame::Basic::ConsumeOk
      delivery_channel = ::Channel(DeliverMessage).new(8192)
      @consumers[ok.consumer_tag] = delivery_channel
      work_pool.times do |i|
        spawn consume(ok.consumer_tag, delivery_channel, blk),
          same_thread: i.zero?, # only force put the first fiber on same thread
          name: "AMQPconsumer##{ok.consumer_tag} ##{i}"
      end
      if block
        cb = @consumer_blocks[ok.consumer_tag] = ::Channel(Exception).new
        if ex = cb.receive?
          write Frame::Basic::Cancel.new(@id, ok.consumer_tag, no_wait: true)
          raise ex
        end
      end
      ok.consumer_tag
    end

    def basic_cancel(consumer_tag) : Nil
      write Frame::Basic::Cancel.new(@id, consumer_tag, no_wait: false)
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
      return if name.empty? # the default exchange cannot be declared
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
      raise ClosedException.new(@closing_frame) if @closing_frame
      @connection.write frame
    end

    private def next_frame : Frame
      @reply_frames.receive
    rescue ex : ::Channel::ClosedError
      raise ClosedException.new(@closing_frame, cause: ex)
    end

    macro expect(clz)
      frame = next_frame
      frame.as?({{ clz }}) || raise UnexpectedFrame.new(frame)
    end

    def inspect(io : IO) : Nil
      io << "#<" << self.class.name << " @id=" << @id << '>'
    end
  end
end
