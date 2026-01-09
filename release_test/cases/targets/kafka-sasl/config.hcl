target {
  use "kafka" {
    brokers         = "broker-sasl:29093"
    topic_name      = "e2e-kafka-sasl-target"

    enable_sasl     = true
    sasl_username   = "testuser"
    sasl_password   = "testuser-password"
    sasl_algorithm  = "plaintext"
  }
}

disable_telemetry = true
