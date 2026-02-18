# Extended configuration for Stdout as a target (all options)

target {
  use "stdout" {
    batching {
      # Maximum number of events that can go into one batched request (default: 1)
      max_batch_messages     = 1
      # Maximum byte limit for a single batched request (default: 1048576)
      max_batch_bytes        = 1000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 1000000
      # How many batches attempted concurrently (default: 1)
      max_concurrent_batches = 1
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 100
    }

    data_only_output = true
  }
}