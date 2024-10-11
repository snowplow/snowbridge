# Extended configuration for PubSub as a source (all options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id        = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id   = "subscription-id"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20

    # Maximum number of unprocessed messages (default 1000)
    max_outstanding_messages = 2000

    # Maximum size of unprocessed messages (default 1e9)
    max_outstanding_bytes = 2e9
  }
}
