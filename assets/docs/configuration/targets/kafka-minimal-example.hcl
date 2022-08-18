# Minimal configuration for Kafka as a target (only required options)

target {
  use "kafka" {
    # Kafka broker connectinon string
    brokers    = "my-kafka-connection-string"

    # Kafka topic name
    topic_name = "snowplow-enriched-good"
  }
}
