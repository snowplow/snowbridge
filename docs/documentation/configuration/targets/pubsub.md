# Configure a PubSub Target

## Authentication

Authentication is done using a [GCP Service Account](https://cloud.google.com/docs/authentication/application-default-credentials#attached-sa). Create a service account credentials file, and provide the path to it via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.


## Configuration options

Here is an example of the minimum required configuration:

TODO: use embed/template of configs/target/minimal/pubsub-minimal.hcl

```hcl
# Configuration of PubSub as a target.

target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}

```

Here is an example of every configuration option:

TODO: use embed/template of configs/target/full/pubsub-full.hcl

```hcl
# Configuration of PubSub as a target.

target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}

```