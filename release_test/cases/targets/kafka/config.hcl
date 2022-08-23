target {
  use "kafka" {
    brokers    = "broker:29092"
    topic_name = "e2e-kafka-target"
  }
}

disable_telemetry = true