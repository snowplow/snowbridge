# Kinesis Target

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the stream.


## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for Kinesis as a target (only required options)

target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.

Here is an example of every configuration option:

```hcl
# Extended configuration for Kinesis as a target (all options)

target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"

    # Optional ARN to use on the stream (default: "")
    role_arn    = "arn:aws:iam::123456789012:role/myrole"
  }
}

```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.