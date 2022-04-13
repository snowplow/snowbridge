# Simple configuration for Kafka as a source (only required options)

target {
  use "kafka" {
    # Kafka broker connection string
    brokers       = "my-kafka-connection-string"

    # Kafka topic name
    topic_name    = "snowplow-enriched-good"

    # Kafka consumer group name
    consumer_name = "snowplow-stream-replicator"
  }
}
