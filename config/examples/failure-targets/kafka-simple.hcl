# Simple configuration for Kafka as a failure target (only required options)

failure_target {
  use "kafka" {
    # Kafka broker connectinon string
    brokers    = "my-kafka-connection-string"

    # Kafka topic name
    topic_name = "snowplow-enriched-good"
  }
}
