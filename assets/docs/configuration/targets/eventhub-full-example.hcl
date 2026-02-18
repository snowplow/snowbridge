# Extended configuration for Eventhub as a target (all options)

target {
  use "eventhub" {
    batching {

      # Maximum number of events that can go into one batched request (default: 500)
      max_batch_messages     = 200
      # Maximum byte limit for a single batched request (default: 1048576)
      max_batch_bytes        = 10000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 10000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 200
    }
    # Namespace housing Eventhub
    namespace                  = "testNamespace"

    # Name of Eventhub
    name                       = "testName"

    # Number of retries handled automatically by the EventHubs library.
    # All retries should be completed before context timeout (default: 1).
    max_auto_retries           = 2

    # The time (seconds) before context timeout (default: 20)
    context_timeout_in_seconds = 21

    # Sets the eventHub message partition key, which is used by the EventHub client's batching strategy
    set_eh_partition_key = true
  }
}
