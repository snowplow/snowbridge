# Extended configuration for Kafka as a target (all options)

target {
  use "kafka" {
    batching {
      # Maximum number of events that can go into one batched request (default: 100)
      max_batch_messages     = 20
      # Maximum byte limit for a single batched request (default: 1048576)
      max_batch_bytes        = 10000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 10000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 200
    }
    # Kafka broker connectinon string
    brokers             = "my-kafka-connection-string"

    # Kafka topic name
    topic_name          = "snowplow-enriched-good"

    # The Kafka version
    target_version      = "2.7.0"

    # Max retries (default: 5)
    max_retries         = 11

    # Whether to compress data (default: false).
    # Reduces network usage and increases latency.
    compress            = true

    # Sets RequireAck s= WaitForAll, which waits for min.insync.replicas
    # to Ack (default: false)
    wait_for_all        = true

    # Exactly once writes - Also sets RequiredAcks = WaitForAll (default: false)
    idempotent          = true

    # Whether to enable SASL support (default: false)
    enable_sasl         = true

    # SASL AUTH
    sasl_username       = "mySaslUsername"
    sasl_password       = env.SASL_PASSWORD

    # The SASL Algorithm to use: "plaintext", "sha512" or "sha256" (default: "sha512")
    sasl_algorithm      = "sha256"

    # The SASL version to use: 0 or 1 (default: 0)
    # 1 recommended for compatible systems
    sasl_version        = 1

    # Whether to enable TLS
    enable_tls = true

    # The optional certificate file for client authentication
    cert_file            = "myLocalhost.crt"

    # The optional key file for client authentication
    key_file             = "myLocalhost.key"

    # The optional certificate authority file for TLS client authentication
    ca_file              = "myRootCA.crt"

    # Whether to skip verifying ssl certificates chain (default: false)
    skip_verify_tls     = true

    # Forces the use of the Sync Producer (default: false).
    # Emits as fast as possible but may limit performance.
    force_sync_producer = true
  }
}
