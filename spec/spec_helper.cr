require "spec"
require "http/client"
require "json"
require "../src/amqp-client"

Log.setup_from_env
{% if Spec::CLI.resolve? %}
  Spec.cli.configure_formatter("verbose") unless ENV["CI"]?
{% else %}
  Spec.configure_formatter("verbose") unless ENV["CI"]?
{% end %}

module TestHelpers
  def with_connection(channel_max = 1024_u16, **args, &)
    AMQP::Client.start(**args, channel_max: channel_max) do |c|
      yield c
    end
  end

  def with_channel(**args, &)
    with_connection(**args) do |c|
      yield c.channel
    end
  end

  def wait_until_closed(c : AMQP::Client::Connection, timeout = 1.second)
    deadline = Time.instant + timeout
    until c.closed?
      fail "the connection didn't close" if Time.instant > deadline
      sleep 10.milliseconds
    end
  end

  def with_ws_connection(&)
    AMQP::Client.start(websocket: true, port: 15_672) do |c|
      yield c
    end
  end

  def with_ws_channel(&)
    with_ws_connection do |c|
      yield c.channel
    end
  end

  struct ManagementApi
    def initialize(uri : URI)
      @http = HTTP::Client.new uri
      username = uri.user || "guest"
      password = uri.password || "guest"
      @http.basic_auth username, password
    end

    def connections
      get("/api/connections/") do |resp|
        JSON.parse(resp.body_io).as_a
      end
    end

    def close_connections(amount : Int)
      loop do
        conns = connections
        conns.each do |conn|
          name = conn["name"].as_s
          delete("/api/connections/#{URI.encode_path_segment name}")
          amount -= 1
        end
        break if amount <= 0
        sleep 1.seconds
      end
    end

    forward_missing_to @http
  end

  {% if Fiber.has_constant?("ExecutionContext") %}
    # Opens a connection from an execution context of its own, so that its
    # read loop runs on another thread than the fiber using the connection
    def with_connection_on_other_thread(&)
      reader = Fiber::ExecutionContext::Concurrent.new("amqp read loop")
      connected = Channel(AMQP::Client::Connection | Exception).new
      reader.spawn do
        connected.send AMQP::Client.new.connect
      rescue ex
        connected.send ex
      end
      conn = connected.receive
      raise conn if conn.is_a?(Exception)
      begin
        yield conn
      ensure
        conn.close
      end
    end
  {% end %}

  # Fails if the block doesn't return within *span*
  def within(span : Time::Span, description : String, &blk : -> _)
    done = Channel(Exception?).new(1)
    spawn do
      blk.call
      done.send nil
    rescue ex
      done.send ex
    end
    select
    when ex = done.receive
      raise ex if ex
    when timeout(span)
      fail "#{description} didn't return within #{span}"
    end
  end

  def with_http_api(&)
    uri = URI.parse ENV.fetch("MGMT_URL", "http://guest:guest@#{AMQP::Client::AMQP_HOST}:15672")
    yield ManagementApi.new(uri)
  end
end

extend TestHelpers
