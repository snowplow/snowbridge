# Extended configuration for PubSub as a source (all options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id        = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id   = "subscription-id"

    # Maximum number of unprocessed messages (default 1000)
    max_outstanding_messages = 2000

    # Maximum size of unprocessed messages (default 1e9)
    max_outstanding_bytes = 2e9

    # Minimum ack extension period when a message is received
    min_extension_period_seconds = 10

    # Number of streaming pull connections to open at once
    streaming_pull_goroutines = 1

    # Configures the GRPC connection pool size of the pubsub client
    grpc_connection_pool_size = 4
  }
}
