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

    class NetworkError < Error
      getter host : String
      getter port : Int32

      def initialize(message : String, @host, @port, cause : Exception)
        super("#{message} (#{@host}:#{@port})", cause)
      end
    end
  end

  class Connection
    class ClosedException < Error
      # True when the client closed the connection itself, with `Connection#close`
      getter? client_initiated = false

      def initialize(message, cause = nil, *, @client_initiated = false)
        super(message, cause)
      end

      def initialize(frame : Frame::Connection::Close, cause : Exception? = nil)
        super("#{frame.reply_text} (#{frame.reply_code})", cause)
      end

      def initialize(message, host, user, vhost)
        super "#{message} host=#{host} user=#{user} vhost=#{vhost}"
      end
    end
  end

  class Channel
    class ClosedException < Error
      # True when the client closed the channel itself, with `Channel#close`
      getter? client_initiated = false

      def initialize(message : String, cause = nil, *, @client_initiated = false)
        super(message, cause)
      end

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
