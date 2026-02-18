source {
  use "kafka" {
    brokers         = "broker:29092"
    topic_name      = "e2e-kafka-source"
    consumer_name   = "e2e-kafka-source-consumer"
    offsets_initial = -2
  }
}

target {
  use "stdout" {
    batching {
      max_batch_messages = 10
    }
  }
}

transform {
  worker_pool = 5
}

disable_telemetry = true
