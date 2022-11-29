require "./connection"
require "./message"
require "./queue"
require "./exchange"
require "./sync"

class AMQP::Client
  class Channel
    private Log = ::Log.for(self)

    @connection : Connection
    @reply_frames = ::Channel(Frame).new
    @basic_get = ::Channel(GetMessage?).new
    @confirm_id = 0_u64
    getter id # Channel ID
    @prefetch_count = 0u16
    @confirm_mode = false
    @server_flow = true
    @closed = false

    # :nodoc:
    def initialize(@connection : Connection, @id : UInt16)
    end

    # :nodoc:
    def open
      write Frame::Channel::Open.new(@id)
      expect Frame::Channel::OpenOk
      self
    end

    @on_close : Proc(UInt16, String, Nil)?

    # Callback that will be called if the channel is closed by the server
    def on_close(&blk : UInt16, String ->)
      @on_close = blk
    end

    # Close the channel
    # The *reason* might be logged by the server
    def close(reason = "", code = 200) : Nil
      return if @closed
      @closed = true
      write Frame::Channel::Close.new(@id, code.to_u16, reason, 0, 0)
      @consumers.each_value &.close
      expect Frame::Channel::CloseOk
      cleanup
    end

    # :nodoc:
    def close(frame : Frame::Channel::Close) : Nil
      begin
        write Frame::Channel::CloseOk.new(@id)
      rescue ex
        Log.error(exception: ex) { "Couldn't write CloseOk frame" }
      end
      @closed = true
      @closing_frame = frame
      Log.info { "Channel closed by server: #{frame.inspect}" } unless @on_close
      begin
        @on_close.try &.call(frame.reply_code, frame.reply_text)
      rescue ex
        Log.error(exception: ex) { "Uncaught exception in on_close block" }
      end
      cleanup
    end

    # Stop/start the flow of messages to consumers
    # Not supported by all brokers
    def flow(active : Bool)
      write Frame::Channel::Flow.new(@id, active)
      expect Frame::Channel::FlowOk
    end

    def closed?
      @closed
    end

    # :nodoc:
    def cleanup
      @closed = true
      @reply_frames.close
      @basic_get.close
      @on_confirm.each_value &.call(false)
      @on_confirm.clear
      @consumers.each_value &.close
      @consumers.clear
    end

    @next_msg_deliver : Frame::Basic::Deliver | Frame::Basic::Return | Frame::Basic::GetOk | Nil
    @next_msg_props : AMQ::Protocol::Properties?
    @next_body_io : IO::Memory?
    @next_body_size = 0_u32

    # :nodoc:
    def incoming(frame) # ameba:disable Metrics/CyclomaticComplexity
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
      when Frame::Basic::Ack
        process_confirm(true, frame.delivery_tag, frame.multiple)
      when Frame::Basic::Nack
        process_confirm(false, frame.delivery_tag, frame.multiple)
      when Frame::Channel::Flow
        process_flow frame.active
      when Frame::Channel::Close
        close frame
      when Frame::Basic::ConsumeOk
        @reply_frames.send frame
        Fiber.yield # yield so that the consumer can be added to the hash before msgs are read
      else
        @reply_frames.send frame
      end
    end

    @on_cancel : Proc(String, Nil)?

    # Callback that will be called if a consumer is cancelled by the server
    # The argument to the callback is the consumer tag
    def on_cancel(&blk : String -> Nil)
      @on_cancel = blk
    end

    private def process_cancel(consumer_tag : String, no_wait : Bool)
      Log.warn { "Consumer #{consumer_tag} cancelled by server" } unless @on_cancel || @closed || @connection.closed?
      begin
        @on_cancel.try &.call(consumer_tag)
      rescue ex
        Log.error(exception: ex) { "Uncaught exception in on_cancel" }
      end
      if deliveries = @consumers.delete(consumer_tag)
        deliveries.close
      else
        Log.warn { "Consumer tag '#{consumer_tag}' already cancelled" }
      end
      write Frame::Basic::CancelOk.new(@id, consumer_tag) unless no_wait
    end

    private def process_cancel_ok(consumer_tag : String)
      if deliveries = @consumers.delete(consumer_tag)
        deliveries.close
      end
    end

    private def process_deliver
      case f = @next_msg_deliver
      when Frame::Basic::Deliver
        if deliveries = @consumers[f.consumer_tag]?
          msg = DeliverMessage.new(self, f.exchange, f.routing_key,
            f.delivery_tag, @next_msg_props.not_nil!, @next_body_io.not_nil!,
            f.redelivered)
          begin
            deliveries.send(msg)
          rescue ::Channel::ClosedError
            Log.debug { "Consumer tag '#{f.consumer_tag}' is cancelled" } unless @closed
          end
        else
          Log.debug { "Consumer tag '#{f.consumer_tag}' not found" } unless @closed
        end
      when Frame::Basic::Return
        msg = ReturnedMessage.new(f.reply_code, f.reply_text,
          f.exchange, f.routing_key, @next_msg_props.not_nil!, @next_body_io.not_nil!)
        if on_return = @on_return
          spawn on_return.call(msg), name: "AMQP::Client::Channel#on_return"
        else
          Log.error { "Message returned but no on_return block defined: #{msg.inspect}" }
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

    @on_return : Proc(ReturnedMessage, Nil)?

    # Callback that is called if a published message is returned by the server
    def on_return(&blk : ReturnedMessage -> Nil)
      @on_return = blk
    end

    private def process_flow(active : Bool)
      @server_flow = active
      write Frame::Channel::FlowOk.new(@id, active)
    end

    # Publish a *bytes* message, to an *exchange* with *routing_key*
    def basic_publish(bytes : Bytes, exchange, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new)
      basic_publish(bytes, bytes.size, exchange, routing_key, mandatory, immediate, properties)
    end

    # Publish a *string* message, to an *exchange* with *routing_key*
    def basic_publish(string : String, exchange, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new)
      basic_publish(string.to_slice, exchange, routing_key, mandatory, immediate, properties)
    end

    # Publish an *io* message, to an *exchange* with *routing_key*
    # Only data from the current position of the IO to the end will be published. The position will be restored after publish.
    def basic_publish(io : (IO::Memory | IO::FileDescriptor), exchange, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new)
      start_pos = io.pos
      begin
        basic_publish(io, io.bytesize - start_pos, exchange, routing_key, mandatory, immediate, properties)
      ensure
        io.pos = start_pos
      end
    end

    # Publish a message with a set *bytesize*, to an *exchange* with *routing_key*
    def basic_publish(body : IO | Bytes, bytesize : Int, exchange : String, routing_key = "",
                      mandatory = false, immediate = false, props properties = Properties.new) : UInt64
      raise ClosedException.new(@closing_frame) if @closing_frame

      @connection.with_lock(flush: !@tx) do |c|
        c.unsafe_write Frame::Basic::Publish.new(@id, 0_u16, exchange, routing_key, mandatory, immediate)
        c.unsafe_write Frame::Header.new(@id, 60_u16, 0_u16, bytesize.to_u64, properties)
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
        @confirm_id = @confirm_id &+ 1_u64
      else
        0_u64
      end
    end

    def basic_publish_confirm(msg, exchange, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(msg, exchange, routing_key, mandatory, immediate, properties)
      wait_for_confirm(msgid)
    end

    def basic_publish_confirm(io : IO, bytesize : Int, exchange : String, routing_key = "", mandatory = false, immediate = false, props properties = Properties.new) : Bool
      confirm_select
      msgid = basic_publish(io, bytesize, exchange, routing_key, mandatory, immediate, properties)
      wait_for_confirm(msgid)
    end

    # Block until confirmed published message with *msgid* returned from `basic_publish`
    def wait_for_confirm(msgid) : Bool
      raise Error.new("Channel not in confirm mode, call `confirm_select` first") unless @confirm_mode

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

    private def process_confirm(acked : Bool, delivery_tag : UInt64, multiple : Bool)
      @last_confirm = {delivery_tag, acked}
      if multiple
        @on_confirm.reject! do |msgid, blk|
          if msgid <= delivery_tag
            blk.call acked
            true
          else
            false
          end
        end
      elsif blk = @on_confirm.delete delivery_tag
        blk.call acked
      end
    end

    # Get a single message from a *queue*
    # The message must eventually be acked or rejected if *no_ack* is false
    def basic_get(queue : String, no_ack = true) : GetMessage?
      write Frame::Basic::Get.new(@id, 0_u16, queue, no_ack)
      @basic_get.receive
    rescue ex : ::Channel::ClosedError
      if cf = @connection.closing_frame
        raise Connection::ClosedException.new(cf)
      elsif cf = @closing_frame
        raise ClosedException.new(cf, cause: ex)
      else
        raise ex
      end
    end

    # :nodoc:
    def has_subscriber?(consumer_tag)
      @consumers.has_key? consumer_tag
    end

    @consumers = Sync(Hash(String, ::Channel(DeliverMessage))).new

    # Consume messages from a *queue*
    #
    # * Make sure to eventually ack or reject each message if *no_ack* is false
    # * The *exclusive* flags ensures that only a single consumer receives messages from the queue at the time
    # * The method will *block* if the flag is set, until the consumer/channel/connection is closed or the callback raises an exception
    # * To let multiple fibers process messages increase *work_pool*
    # Make sure to handle all exceptions in the consume block,
    # as unhandeled exceptions will cause the channel to be closed (to prevent dangling unacked messages and exception floods).
    def basic_consume(queue, tag = "", no_ack = true, exclusive = false,
                      block = false, args arguments = Arguments.new, work_pool = 1,
                      &blk : DeliverMessage -> Nil)
      raise ArgumentError.new("Max allowed work_pool is 1024") if work_pool > 1024
      raise ArgumentError.new("At least one worker required") if work_pool < 1

      write Frame::Basic::Consume.new(@id, 0_u16, queue, tag, false, no_ack, exclusive, false, arguments)
      ok = expect Frame::Basic::ConsumeOk
      done = ::Channel(Exception?).new
      deliveries = ::Channel(DeliverMessage).new(@prefetch_count.to_i32)
      @consumers[ok.consumer_tag] = deliveries
      work_pool.times do |i|
        spawn consume(ok.consumer_tag, deliveries, done, i, blk),
          same_thread: i.zero?, # only force put the first fiber on same thread
          name: "AMQPconsumer##{ok.consumer_tag} ##{i}"
      end
      if block
        work_pool.times do
          if ex = done.receive
            done.close
            raise ex
          end
        end
      else
        done.close
      end
      ok.consumer_tag
    end

    private def consume(consumer_tag, deliveries, done, i, blk)
      Log.context.set channel_id: @id.to_i, consumer: consumer_tag, worker: i
      while msg = deliveries.receive?
        begin
          blk.call(msg)
        rescue ex
          Log.error(exception: ex) { "Uncaught exception in consumer, closing channel" }
          close("Uncaught exception in consumer #{consumer_tag}", 500)
          done.send(ex) rescue nil
          return
        end
      end
      done.send(nil) rescue nil
    end

    # Cancel the consumer with the *consumer_tag*
    # Even with *no_wait* = false the method will return immediately, but outstanding deliveries will be processed.
    def basic_cancel(consumer_tag, no_wait = false) : Nil
      if @consumers.has_key? consumer_tag
        write Frame::Basic::Cancel.new(@id, consumer_tag, no_wait)
        if no_wait
          if deliveries = @consumers.delete(consumer_tag)
            deliveries.close
          end
        end
      else
        Log.info { "Consumer tag '#{consumer_tag}' already cancelled" }
      end
    end

    # Acknowledge a message with *delivery_tag*,
    # or all message up and including *delivery_tag* if *multiple* set set
    def basic_ack(delivery_tag : UInt64, multiple = false) : Nil
      write Frame::Basic::Ack.new(@id, delivery_tag, multiple)
    end

    # Reject a message with *delivery_tag*, optionally *requeue* it
    def basic_reject(delivery_tag : UInt64, requeue = false) : Nil
      write Frame::Basic::Reject.new(@id, delivery_tag, requeue)
    end

    # Reject a message with *delivery_tag*, optionally *requeue* it
    # Reject all message up to and including *delivery_tag* if *multiple* is true
    def basic_nack(delivery_tag : UInt64, requeue = false, multiple = false) : Nil
      write Frame::Basic::Nack.new(@id, delivery_tag, multiple, requeue)
    end

    # Set prefetch limit to *count* messages,
    # no more messages will be delivered to the consumer until one or more message have been acknowledged or rejected
    def basic_qos(count, global = false) : Nil
      write Frame::Basic::Qos.new(@id, 0_u32, count.to_u16, global)
      expect Frame::Basic::QosOk
      @prefetch_count = count.to_u16
    end

    # Alias for `basic_qos`
    def prefetch(count, global = false) : Nil
      basic_qos(count, global)
    end

    # Declares a temporary queue, which won't be durable and auto-deleted when not used anymore
    def queue
      q = queue_declare("", durable: false, auto_delete: true, exclusive: true)
      Queue.new(self, q[:queue_name])
    end

    # Declares a queue with a name, by default durable and not auto-deleted
    def queue(name : String, passive = false, durable = true, exclusive = false, auto_delete = false, args arguments = Arguments.new)
      q = queue_declare(name, passive, durable, exclusive, auto_delete, arguments)
      Queue.new(self, q[:queue_name])
    end

    # Declare a queue with *name*
    # *passive* will raise if the queue doesn't already exists, other arguments are ignored
    # *durable* will make the queue durable on the server (note that messages have have the persistent flag set to make the messages persistent)
    # *exclusive* will make the queue exclusive to the channel and will be deleted when the channel is closed
    # *auto_delete* will delete the queue when the last consumer as stopped consuming
    def queue_declare(name : String, passive = false,
                      durable = name.empty? ? false : true,
                      exclusive = name.empty? ? true : false,
                      auto_delete = name.empty? ? true : false,
                      args arguments = Arguments.new)
      no_wait = false
      write Frame::Queue::Declare.new(@id, 0_u16, name, passive, durable, exclusive, auto_delete, no_wait, arguments)
      f = expect Frame::Queue::DeclareOk
      {
        queue_name:     f.queue_name,
        message_count:  f.message_count,
        consumer_count: f.consumer_count,
      }
    end

    # Delete a queue
    def queue_delete(name : String, if_unused = false, if_empty = false)
      write Frame::Queue::Delete.new(@id, 0_u16, name, if_unused, if_empty, no_wait: false)
      f = expect Frame::Queue::DeleteOk
      {message_count: f.message_count}
    end

    # Purge/empty a queue, will return the number of messages deleted
    def queue_purge(name : String)
      write Frame::Queue::Purge.new(@id, 0_u16, name, no_wait: false)
      f = expect Frame::Queue::PurgeOk
      {message_count: f.message_count}
    end

    # Bind a *queue* to an *exchange*, with a *routing_key* and optionally some *arguments*
    def queue_bind(queue : String, exchange : String, routing_key : String, no_wait = false, args arguments = Arguments.new) : Nil
      write Frame::Queue::Bind.new(@id, 0_u16, queue, exchange, routing_key, no_wait, arguments)
      expect Frame::Queue::BindOk unless no_wait
    end

    # Unbind a *queue* from an *exchange*, with a *routing_key* and optionally some *arguments*
    def queue_unbind(queue : String, exchange : String, routing_key : String, args arguments = Arguments.new) : Nil
      write Frame::Queue::Unbind.new(@id, 0_u16, queue, exchange, routing_key, arguments)
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

    # Convinence method for Exchange handling
    def exchange(name, type, passive = false, durable = true,
                 internal = false, auto_delete = false, args arguments = Arguments.new)
      exchange_declare(name, type, passive, durable, internal, auto_delete, false, arguments)
      Exchange.new(self, name)
    end

    # Declares an exchange
    def exchange_declare(name : String, type : String, passive = false,
                         durable = true, internal = false, auto_delete = false,
                         no_wait = false, args arguments = Arguments.new) : Nil
      return if name.empty? # the default exchange cannot be declared
      write Frame::Exchange::Declare.new(@id, 0_u16, name, type, passive,
        durable, auto_delete, internal,
        no_wait, arguments)
      expect Frame::Exchange::DeclareOk unless no_wait
    end

    # Deletes an exchange
    def exchange_delete(name, if_unused = false, no_wait = false) : Nil
      write Frame::Exchange::Delete.new(@id, 0_u16, name, if_unused, no_wait)
      expect Frame::Exchange::DeleteOk unless no_wait
    end

    # Bind an exchange to another exchange
    def exchange_bind(source : String, destination : String, routing_key : String, no_wait = false, args arguments = Arguments.new) : Nil
      write Frame::Exchange::Bind.new(@id, 0_u16, source, destination, routing_key, no_wait, arguments)
      expect Frame::Exchange::BindOk unless no_wait
    end

    # Unbind an exchange from another exchange
    def exchange_unbind(source : String, destination : String, routing_key : String, no_wait = false, args arguments = Arguments.new) : Nil
      write Frame::Exchange::Unbind.new(@id, 0_u16, source, destination, routing_key, no_wait, arguments)
      expect Frame::Queue::UnbindOk unless no_wait
    end

    # Sets the channel in publish confirm mode, each published message will be acked or nacked
    def confirm_select(no_wait = false) : Nil
      return if @confirm_mode
      write Frame::Confirm::Select.new(@id, no_wait)
      expect Frame::Confirm::SelectOk unless no_wait
      @confirm_mode = true
    end

    # Tell the broker to either deliver all unacknowledge messages again if *requeue* is false or rejecting all if *requeue* is true
    #
    # Unacknowledged messages retrived by `basic_get` are requeued regardless.
    def basic_recover(requeue : Bool) : Nil
      write Frame::Basic::Recover.new(@id, requeue)
      expect Frame::Basic::RecoverOk
    end

    @tx = false

    # Set the Channel in transaction mode
    def tx_select : Nil
      return if @tx
      write Frame::Tx::Select.new(@id)
      expect Frame::Tx::SelectOk
      @tx = true
    end

    # Commit a transaction
    def tx_commit : Nil
      write Frame::Tx::Commit.new(@id)
      expect Frame::Tx::CommitOk
    end

    # Rollback a transaction
    def tx_rollback : Nil
      write Frame::Tx::Rollback.new(@id)
      expect Frame::Tx::RollbackOk
    end

    # Commits a transaction if the block returns,
    # rolls back the transaction if the block raises an exception
    def transaction
      tx_select
      begin
        yield
      rescue ex
        tx_rollback
        raise ex
      else
        tx_commit
      end
    end

    private def write(frame)
      raise ClosedException.new(@closing_frame) if @closing_frame
      @connection.write frame
    end

    private def next_frame : Frame
      @reply_frames.receive
    rescue ex : ::Channel::ClosedError
      if conn_close = @connection.closing_frame
        raise Connection::ClosedException.new(conn_close)
      else
        raise ClosedException.new(@closing_frame, cause: ex)
      end
    end

    private macro expect(clz)
      frame = next_frame
      frame.as?({{ clz }}) || raise Error::UnexpectedFrame.new(frame)
    end

    def inspect(io : IO) : Nil
      io << "#<" << self.class.name << " @id=" << @id << '>'
    end
  end
end
