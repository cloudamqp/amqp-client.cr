require "http/web_socket"

class AMQP::Client
  # :nodoc:
  class WebSocketIO < ::IO
    include IO::Buffered

    def initialize(@ws : ::HTTP::WebSocket)
      @r, @w = IO.pipe
      @r.read_buffering = false
      @w.sync = true
      @ws.on_binary do |bytes|
        @w.write(bytes)
      end
      @ws.on_close do |_code, _message|
        self.close
      end
      self.buffer_size = 4096
      spawn @ws.run, name: "websocket run"
    end

    def unbuffered_read(slice : Bytes)
      @r.read(slice)
    end

    def unbuffered_write(slice : Bytes) : Nil
      @ws.send(slice)
    end

    def read_timeout=(timeout)
      @r.read_timeout = timeout
    end

    def unbuffered_rewind
    end

    def unbuffered_flush
    end

    def unbuffered_close : Nil
      return if @closed
      @closed = true
      @r.close
      @w.close
      @ws.close
    end
  end
end
