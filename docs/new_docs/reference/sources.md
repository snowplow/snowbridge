Source names currently supported are:

1. `stdin`: Allows for easy debugging
2. `kinesis`: Replicates from an Amazon Kinesis Stream
3. `pubsub`: Replicates from a GCP PubSub Topic
4. `sqs`: Replicates from an Amazon SQS Queue

## Configuration via file:

If configuring via `.hcl` file, use a `source {}` block, and choose a source using `use "{source_name}" {}`. Accepted arguments for each source's `use` block can be found in the `hcl` tags in the "Configuration API" section below.

Example:

```
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

    # Number of events to process concurrently (default: 50)
    concurrent_writes = 15
  }
}
```

## Configuration via environment variables

If configuring via environment variable, choose a source by setting `SOURCE_NAME={source_name}`. Accepted environment variables for each source's arguments can be found in the `env` tags in the "Configuration API" section below.

Example:

```bash
export SOURCE_NAME="kinesis"                                    \
SOURCE_KINESIS_STREAM_NAME="my-stream"                          \
SOURCE_KINESIS_REGION="us-west-1"                               \
SOURCE_KINESIS_APP_NAME="StreamReplicatorProd1"                 \
SOURCE_KINESIS_ROLE_ARN="arn:aws:iam::123456789012:role/myrole" \
SOURCE_KINESIS_START_TSTAMP="2020-01-01 10:00:00"               \
SOURCE_CONCURRENT_WRITES=15
```

## Authorisation

Authorisation for AWS And GCP cloud technologies are done by leveraging the default Cloud Environment variables (where appropriate).  Each Cloud provides documentation on how this works:

1. [AWS CLI Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)
2. [GCP Service Account](https://cloud.google.com/docs/authentication/getting-started)

## Kinesis source setup

The AWS kinesis source requires the additional setup of a set of dynamoDB tables for checkpointing purposes. To set up a kinesis source, you will need to:

1. Configure the above required variables in the HCL file.
2. Create three DynamoDB tables which will be used for checkpointing the progress of the replicator on the stream (*Note*: details below)

Under the hood we are using a fork of the [Twitch Kinsumer](https://github.com/snowplow-devops/kinsumer) library which has defined this DynamoDB table structure - these tables need to be made by hand before the application can launch.

| TableName                                | DistKey        |
|------------------------------------------|----------------|
| `${SOURCE_KINESIS_APP_NAME}_clients`     | ID (String)    |
| `${SOURCE_KINESIS_APP_NAME}_checkpoints` | Shard (String) |
| `${SOURCE_KINESIS_APP_NAME}_metadata`    | Key (String)   |

Assuming your AWS credentials have sufficient permission to Kinesis and DynamoDB your consumer should now be able to run by launching the executable.

## Configuration API

Available sources and their options are configured as detailed below. `hcl:` tags specify the hcl option name, `env:` tags specify the environment variable name.

<details>
<summary>Kinesis</summary>
<pre><code>type configuration struct {
	StreamName       string `hcl:"stream_name" env:"SOURCE_KINESIS_STREAM_NAME"`
	Region           string `hcl:"region" env:"SOURCE_KINESIS_REGION"`
	AppName          string `hcl:"app_name" env:"SOURCE_KINESIS_APP_NAME"`
	RoleARN          string `hcl:"role_arn,optional" env:"SOURCE_KINESIS_ROLE_ARN"`
	StartTimestamp   string `hcl:"start_timestamp,optional" env:"SOURCE_KINESIS_START_TIMESTAMP"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}</code></pre>
</details>

<details>
<summary>PubSub</summary>
<pre><code>type configuration struct {
	ProjectID        string `hcl:"project_id" env:"SOURCE_PUBSUB_PROJECT_ID"`
	SubscriptionID   string `hcl:"subscription_id" env:"SOURCE_PUBSUB_SUBSCRIPTION_ID"`
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}</code></pre>
</details>

<details>
<summary>SQS</summary>
<pre><code>type configuration struct {
	QueueName        string `hcl:"queue_name" env:"SOURCE_SQS_QUEUE_NAME"`
	Region           string `hcl:"region" env:"SOURCE_SQS_REGION"`
	RoleARN          string `hcl:"role_arn,optional" env:"SOURCE_SQS_ROLE_ARN"`
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}</code></pre>
</details>

<details>
<summary>stdin</summary>
<pre><code>type configuration struct {
	ConcurrentWrites int `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}</code></pre>
</details>


