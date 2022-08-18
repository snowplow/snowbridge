# Minimal configuration for Kinesis as a target (only required options)

target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"
  }
}
