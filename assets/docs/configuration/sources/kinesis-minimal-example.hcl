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