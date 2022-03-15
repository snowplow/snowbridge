# Extended configuration for PubSub as a source (all options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id        = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id   = "subscription-id"

    # Number of events to process concurrently (default: 50)
    concurrent_writes = 20
  }
}
