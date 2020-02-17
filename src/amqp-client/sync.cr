class AMQP::Client
  struct Sync(T)
    @obj : T
    @mutex = Mutex.new

    def initialize(@obj : T = T.new)
    end

    macro method_missing(call)
      @mutex.synchronize do
        @obj.{{call}}
      end
    end
  end
end
