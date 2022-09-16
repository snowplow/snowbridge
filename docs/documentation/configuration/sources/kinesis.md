# Kinesis Source

## Authentication

Authentication is done via the [AWS authentication environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html). Optionally, you can use the `role_arn` option to specify an ARN to use on the stream.

## Setup

The AWS kinesis source requires the additional setup of a set of dynamoDB tables for checkpointing purposes. To set up a kinesis source, you will need to:

1. Configure the above required variables in the HCL file.
2. Create three DynamoDB tables which will be used for checkpointing the progress of the replicator on the stream (*Note*: details below)

Under the hood we are using a fork of the [Twitch Kinsumer](https://github.com/snowplow-devops/kinsumer) library which has defined this DynamoDB table structure - these tables need to be created by hand before the application can launch.

| TableName                                | DistKey        |
|------------------------------------------|----------------|
| `${SOURCE_KINESIS_APP_NAME}_clients`     | ID (String)    |
| `${SOURCE_KINESIS_APP_NAME}_checkpoints` | Shard (String) |
| `${SOURCE_KINESIS_APP_NAME}_metadata`    | Key (String)   |

Assuming your AWS credentials have sufficient permission for Kinesis and DynamoDB, your consumer should now be able to run when you launch the executable.

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for Kinesis as a source (only required options)

source {
  use "kinesis" {
    # Kinesis stream name to read from
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"

    # App name for Stream Replicator
    app_name    = "StreamReplicatorProd1"
  }
}
```

Here is an example of every configuration option:

```hcl
# Extended configuration for Kinesis as a source (all options)

source {
  use "kinesis" {
    # Kinesis stream name to read from (required)
    stream_name       = "my-stream"

    # AWS region of Kinesis stream (required)
    region            = "us-west-1"

    # App name for Stream Replicator (required)
    app_name          = "StreamReplicatorProd1"

    # Optional ARN to use on source stream (default: "")
    role_arn          = "arn:aws:iam::123456789012:role/myrole"

    # Timestamp for the kinesis shard iterator to begin processing.
    # Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
    # (default: TRIM_HORIZON)
    start_timestamp   = "2020-01-01 10:00:00"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 15
  }
}
```