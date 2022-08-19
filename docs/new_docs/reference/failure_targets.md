As with the regular targets we also need to handle "bad" or "failed" data in the stream.  The two factors that stream-replicator handles are:

1. Oversized payloads (i.e. the source has pulled a message too big to send to the target) and;
2. Invalid payloads (i.e. the source has pulled a message that contains invalid content for the target

Failure targets for Stream replicator are the same as targets, except they are only used in the above cases, and they pre-format the failure data. Configuration options for failure targets are exactly the same as those found in [targets](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Targets), with the exception that they are provided in a `failure_target {}` block, and one may provide a `format` option. 

Currently the only `format` supported is a Snowplow Bad row, which is the default.

Example:

```hcl
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }

```
### Configuration via environment variable

Configure the `format` variable with `FAILURE_TARGETS_FORMAT`. Otherwise, the environment variables for failure targets are the same as those for targets, with the prefix `FAILURE_`.

Example:

```bash
export FAILURE_TARGET_NAME="pubsub"                 \
FAILURE_TARGET_PUBSUB_PROJECT_ID="acme-project"     \
FAILURE_TARGET_PUBSUB_TOPIC_NAME="some-acme-topic
```