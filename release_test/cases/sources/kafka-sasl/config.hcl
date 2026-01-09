source {
  use "kafka" {
    brokers         = "broker-sasl:29093"
    topic_name      = "e2e-kafka-sasl-source"
    consumer_name   = "e2e-kafka-sasl-consumer"
    offsets_initial = -2

    enable_sasl     = true
    sasl_username   = "testuser"
    sasl_password   = "testuser-password"
    sasl_algorithm  = "plaintext"
  }
}

disable_telemetry = true
