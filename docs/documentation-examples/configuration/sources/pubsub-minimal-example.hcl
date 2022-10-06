# Minimal configuration for PubSub as a source (only required options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id      = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id = "subscription-id"
  }
}