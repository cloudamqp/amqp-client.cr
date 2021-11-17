require "spec"
require "http/client"
require "json"
require "../src/amqp-client"

Log.setup_from_env
Spec.override_default_formatter(Spec::VerboseFormatter.new) unless ENV["CI"]?

module TestHelpers
  def with_connection(**args)
    AMQP::Client.start(**args) do |c|
      yield c
    end
  end

  def with_channel(**args, &blk)
    with_connection(**args) do |c|
      yield c.channel
    end
  end

  def with_ws_connection
    AMQP::Client.start(websocket: true, port: 15672) do |c|
      yield c
    end
  end

  def with_ws_channel(&blk)
    with_ws_connection do |c|
      yield c.channel
    end
  end
end

extend TestHelpers
