# SQS Source

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the stream.

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for SQS as a source (only required options)

source {
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
# Extended configuration for SQS as a source (all options)

source {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"

    # Role ARN to use on source queue
    role_arn   = "arn:aws:iam::123456789012:role/myrole"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20
  }
}
```