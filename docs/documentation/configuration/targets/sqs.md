# SQS Target

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the queue.


## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for SQS as a target (only required options)

target {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.

Here is an example of every configuration option:

```hcl
# Extended configuration for SQS as a target (all options)

target {
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

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.