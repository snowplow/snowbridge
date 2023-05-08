source {
  use "kafka" {
    brokers         = "broker:29092"
    topic_name      = "e2e-kafka-source"
    consumer_name   = "e2e-kafka-source-consumer"
    offsets_initial = -2
  }
}

disable_telemetry = true
