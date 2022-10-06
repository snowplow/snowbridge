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

