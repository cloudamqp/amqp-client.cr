require "spec"
require "http/client"
require "json"
require "../src/amqp-client"

Log.setup_from_env
{% if Spec::CLI.resolve? %}
  Spec.cli.override_default_formatter(Spec::VerboseFormatter.new) unless ENV["CI"]?
{% else %}
  Spec.override_default_formatter(Spec::VerboseFormatter.new) unless ENV["CI"]?
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
    def initialize(host, port)
      @http = HTTP::Client.new("localhost", 15672)
      @http.basic_auth "guest", "guest"
    end

    def connections
      get("/api/connections") do |resp|
        JSON.parse(resp.body_io).as_a
      end
    end

    def close_all_connections
      connections.each do |conn|
        name = conn["name"].as_s
        delete("/api/connections/#{URI.encode_path_segment name}")
      end
    end

    forward_missing_to @http
  end

  def with_http_api(&)
    yield ManagementApi.new("localhost", 15671)
  end
end

extend TestHelpers
