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
  def connect
    AMQP::Client.new("amqp://guest:guest@localhost", LOG_LEVEL).connect
  end
end

extend TestHelpers
