# frozen_string_literal: true

require "kafka/datadog"
require "fake_datadog_agent"

describe Kafka::Datadog do
  context "when host and port are specified" do
    it "emits metrics to the Datadog agent" do
      agent = FakeDatadogAgent.new
      agent.start
      Kafka::Datadog.socket_path = nil
      Kafka::Datadog.host = agent.host
      Kafka::Datadog.port = agent.port

      client = Kafka::Datadog.statsd

      client.increment("greetings")

      agent.wait_for_metrics

      expect(agent.metrics.count).to eq 1

      metric = agent.metrics.first

      expect(metric).to eq "ruby_kafka.greetings"
      agent.stop
    end
  end

  context "when socket_path is specified" do
    it "emits metrics to the Datadog agent" do
      agent = FakeDatadogAgent.new
      agent.start
      Kafka::Datadog.socket_path = agent.socket_path
      Kafka::Datadog.host = nil
      Kafka::Datadog.port = nil

      client = Kafka::Datadog.statsd

      client.increment("greetings")

      agent.wait_for_metrics

      expect(agent.metrics.count).to eq 1

      metric = agent.metrics.first

      expect(metric).to eq "ruby_kafka.greetings"
      agent.stop
    end
  end
end
