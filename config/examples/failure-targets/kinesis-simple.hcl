# Simple configuration of Kinesis as a failure target (only required options)

failure_target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"
  }
}
