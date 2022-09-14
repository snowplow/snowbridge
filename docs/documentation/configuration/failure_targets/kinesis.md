# Kinesis Failure Target

Failure targets are only used when stream replicator hits an unrecoverable failure. In such cases, errors are sent to the configured failure target, for debugging.

Apart from the fact that the app only sends information about unrecoverable failures to them, failure targets are the same as targets in all other respects.

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the stream.

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for Kinesis as a failure target (only required options)

failure_target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"
  }
}
```

Here is an example of every configuration option:

```hcl
# Extended configuration for Kinesis as a failure target (all options)

failure_target {
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