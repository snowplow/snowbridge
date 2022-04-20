# Extended configuration for Kafka as a target (all options)

target {
  use "kafka" {
    # Kafka broker connectinon string
    brokers             = "my-kafka-connection-string"

    # Kafka topic name
    topic_name          = "snowplow-enriched-good"

    # The Kafka version
    target_version      = "2.7.0"

    # Max retries (default: 10)
    max_retries         = 10

    # Kafka default byte limit is 1MB (default: 1048576)
    byte_limit          = 1048576

    # Whether to compress data (default: false).
    # Reduces network usage and increases latency.
    compress            = true

    # Sets RequireAck s= WaitForAll, which waits for min.insync.replicas
    # to Ack (default: false)
    wait_for_all        = true

    # Exactly once writes - Also sets RequiredAcks = WaitForAll (default: false)
    idempotent          = true

    # Whether to enable SASL support (defailt: false)
    enable_sasl         = true

    # SASL AUTH
    sasl_username       = "mySaslUsername"
    sasl_password       = env.SASL_PASSWORD

    # The SASL Algorithm to use: "sha512" or "sha256" (default: "sha512")
    sasl_algorithm      = "sha256"

    # The optional certificate file for client authentication
    tls_cert           = "dGVzdCBzdHJpbmc="

    # The optional key file for client authentication
    tls_key            = "c29tZSBzdHJpbmc="

    # The optional certificate authority file for TLS client authentication
    tls_ca             = "b3RoZXIgc3RyaW5ncw=="

    # Whether to skip verifying ssl certificates chain (default: false)
    skip_verify_tls     = true

    # Forces the use of the Sync Producer (default: false).
    # Emits as fast as possible but may limit performance.
    force_sync_producer = true

    # Milliseconds between flushes of events (default: 0)
    # Setting to 0 means as fast as possible.
    flush_frequency     = 2

    # Best effort for how many messages are sent in each batch (default: 0)
    # Setting to 0 means as fast as possible.
    flush_messages      = 2

    # Best effort for how many bytes will trigger a flush (default: 0)
    # Setting to 0 means as fast as possible.
    flush_bytes         = 2
  }
}
