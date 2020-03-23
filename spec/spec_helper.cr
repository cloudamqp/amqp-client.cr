require "spec"
require "../src/amqp-client"

{% if flag?(:verbose) %}
  LOG_LEVEL = Logger::DEBUG
{% elsif flag?(:warn) %}
  LOG_LEVEL = Logger::WARN
{% else %}
  LOG_LEVEL = Logger::ERROR
{% end %}

Spec.override_default_formatter(Spec::VerboseFormatter.new)

module TestHelpers

  def with_connection(**args)
    AMQP::Client.start(**args.merge(log_level: LOG_LEVEL)) do |c|
      yield c
    end
  end

  def with_channel(**args, &blk)
    with_connection(**args) do |c|
      yield c.channel
    end
  end
end

extend TestHelpers
