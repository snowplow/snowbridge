# Extended configuration for Kafka as a source (all options)

target {
  use "kafka" {
    # Kafka broker connectinon string
    brokers             = "my-kafka-connection-string"

    # Kafka topic name
    topic_name          = "snowplow-enriched-good"

    # Kafka consumer group name
    consumer_name       = "snowplow-stream-replicator"

    concurrent_writes   = "15"

    # Kafka assignor
    assignor            = "sticky"

    # The Kafka version
    target_version      = "2.7.0"

    # Whether to enable SASL support (defailt: false)
    enable_sasl         = true

    # SASL AUTH
    sasl_username       = "mySaslUsername"
    sasl_password       = env.SASL_PASSWORD

    # The SASL Algorithm to use: "sha512" or "sha256" (default: "sha512")
    sasl_algorithm      = "sha256"

    # The optional certificate file for client authentication
    cert_file           = "myLocalhost.crt"

    # The optional key file for client authentication
    key_file            = "MyLocalhost.key"

    # The optional certificate authority file for TLS client authentication
    ca_file             = "myRootCA.crt"

    # Whether to skip verifying ssl certificates chain (default: false)
    skip_verify_tls     = true
  }
}
