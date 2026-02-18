# Extended configuration for PubSub as a target (all options)

target {
  use "pubsub" {
    batching {
      # Maximum number of events that can go into one batched request (default: 100)
      max_batch_messages     = 20
      # Maximum byte limit for a single batched request (default: 10485760)
      max_batch_bytes        = 10000000
      # Maximum byte limit for individual message (default: 10485760)
      max_message_bytes      = 10000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 200
    }

    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"

    # Optional: Path to service account JSON credentials file
    # If not provided, uses Google Application Default Credentials
    credentials_path = "/path/to/service-account.json"
  }
}
