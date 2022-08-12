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

    # Number of events to process concurrently (default: 50)
    concurrent_writes = 15

    # Delay between tests for the client or shard numbers changing (in seconds)
    shard_check_frequency_seconds   = 60

    # Time between leader actions (in seconds)
    leader_action_frequency_seconds = 60

    # Max age for client record before we consider it stale (in seconds)
    client_record_max_age_seconds   = 120
  }
}
