# PubSub Source

## Authentication

Authentication is done using a [GCP Service Account](https://cloud.google.com/docs/authentication/application-default-credentials#attached-sa). Create a service account credentials file, and provide the path to it via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for PubSub as a source (only required options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id      = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id = "subscription-id"
  }
}
```

Here is an example of every configuration option:

```hcl
# Extended configuration for PubSub as a source (all options)

source {
  use "pubsub" {
    # GCP Project ID
    project_id        = "project-id"

    # subscription ID for the pubsub subscription
    subscription_id   = "subscription-id"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20
  }
}
```