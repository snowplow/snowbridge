# PubSub Target

## Authentication

Authentication is done using a [GCP Service Account](https://cloud.google.com/docs/authentication/application-default-credentials#attached-sa). Create a service account credentials file, and provide the path to it via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.


## Configuration options

The PubSub Target has only two required options, and no optional ones.

```hcl
# Extended configuration for PubSub as a target (all options)

target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.
