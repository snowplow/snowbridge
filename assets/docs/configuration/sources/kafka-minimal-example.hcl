# Simple configuration for Kafka as a source (only required options)
source {
  use "kafka" {
    # Kafka broker connection string
    brokers = "my-kafka-connection-string"

    # Kafka topic name
    topic_name = "snowplow-enriched-good"

    # Kafka consumer group name
    consumer_name = "snowplow-stream-replicator"

    # Kafka offset configuration, -1 stands for read all new messages, -2 stands for read oldest offset that is still available on the broker
    offsets_initial = -2
  }
}
