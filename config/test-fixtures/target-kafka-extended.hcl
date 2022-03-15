# kafka target extended configuration

target {
  use "kafka" {
    brokers             = "testBrokers"
    topic_name          = "testTopic"
    target_version      = "1.2.3"
    max_retries         = 11
    byte_limit          = 1000000
    compress            = true
    wait_for_all        = true
    idempotent          = true
    enable_sasl         = true
    sasl_username       = "testUsername"
    sasl_password       = "testPass"
    sasl_algorithm      = "sha256"
    cert_file           = "test.cert"
    key_file            = "test.key"
    ca_file             = "test.ca"
    skip_verify_tls     = true
    force_sync_producer = true
    flush_frequency     = 2
    flush_messages      = 2
    flush_bytes         = 2
  }
}
