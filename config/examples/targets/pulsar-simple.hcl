# Simple configuration for Pulsar as a target (only required options)

target {
  use "pulsar" {
    # Pulsar broker service connection string
    broker_service_url    = "pulsar://127.0.0.1:6650"

    # Pulsar topic name
    topic_name = "snowplow-enriched-good"
  }
}
