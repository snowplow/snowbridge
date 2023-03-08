# Extended configuration for Kinesis as a source (all options)

source {
  use "kinesis" {
    # Kinesis stream name to read from (required)
    stream_name       = "my-stream"

    # AWS region of Kinesis stream (required)
    region            = "us-west-1"

    # App name for Snowbridge (required)
    app_name          = "SnowbridgeProd1"

    # Optional ARN to use on source stream (default: "")
    role_arn          = "arn:aws:iam::123456789012:role/myrole"

    # Timestamp for the kinesis shard iterator to begin processing.
    # Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
    # (default: TRIM_HORIZON)
    start_timestamp   = "2020-01-01 10:00:00"

    # Optional custom endpoint url to override aws endpoints,
    # this is for use with local testing tools like localstack - don't set for production use.
    custom_aws_endpoint = "http://integration-localstack-1:4566"

    # Optional configurable amount of time to wait when we hit a retryable error on reading from the kinesis stream.
    # This option exists to prevent readThroughputExceeded errors on the source stream when there are other consumers on the stream.
    # Default is 250, cannot be set to lower than 200.
    read_throttle_delay_ms = 500

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 15
  }
}