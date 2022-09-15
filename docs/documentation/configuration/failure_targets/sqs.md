# SQS Target

Failure targets are only used when stream replicator hits an unrecoverable failure. In such cases, errors are sent to the configured failure target, for debugging.

Apart from the fact that the app only sends information about unrecoverable failures to them, failure targets are the same as targets in all other respects.

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the queue.


## Configuration options

Here is an example of the minimum required configuration:

// TODO: add example configs and tests, and template for all of this.

```hcl
# Minimal configuration for SQS as a failure target (only required options)

failure_target {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}
```

Here is an example of every configuration option:


```hcl
# Extended configuration for SQS as a failure target (all options)

failure_target {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"

    # Role ARN to use on SQS queue
    role_arn   = "arn:aws:iam::123456789012:role/myrole"
  }
}
```