target {
  use "kafka" {
    brokers    = "broker:29092"
    topic_name = "e2e-kafka-target"
  }
}

transform {
  worker_pool = 1
}

disable_telemetry = true