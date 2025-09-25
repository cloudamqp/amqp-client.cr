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
  def with_connection(**args, &)
    AMQP::Client.start(**args) do |c|
      yield c
    end
  end

  def with_channel(**args, &)
    with_connection(**args) do |c|
      yield c.channel
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

  def with_http_api(&)
    uri = URI.parse ENV.fetch("MGMT_URL", "http://guest:guest@#{AMQP::Client::AMQP_HOST}:15672")
    yield ManagementApi.new(uri)
  end
end

extend TestHelpers
