class AMQP::Client
  class Error < Exception
    class UnexpectedFrame < Error
      def initialize
        super
      end

      def initialize(frame : Frame)
        super(frame.inspect)
      end
    end
  end

  class Connection
    class ClosedException < Error
      def initialize(message, cause)
        super(message, cause)
      end

      def initialize(frame : Frame::Connection::Close)
        super("#{frame.reply_text} (#{frame.reply_code})")
      end

      def initialize(message, host, user, vhost)
        super "#{message} host=#{host} user=#{user} vhost=#{vhost}"
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
