class AMQP::Client
  class Error < Exception
  end

  class UnexpectedFrame < Error
    def initialize
      super
    end

    def initialize(frame : Frame)
      super(frame.inspect)
    end
  end

  class Connection
    class ClosedException < Error
      def initialize(message, cause)
        super(message, cause)
      end

      def initialize(close : Frame::Connection::Close)
        super("#{close.reply_code} - #{close.reply_text}")
      end
    end
  end

  class Channel
    class ClosedException < Error
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
