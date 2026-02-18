# Extended configuration for Kinesis as a target (all options)

target {
  use "kinesis" {
    batching {
      # Maximum number of events that can go into one batched request (default: 500)
      max_batch_messages     = 200
      # Maximum byte limit for a single batched request (default: 5242880)
      max_batch_bytes        = 5000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 1000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 200
    }
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"

    # Optional custom endpoint url to override aws endpoints,
    # this is for use with local testing tools like localstack - don't set for production use.
    custom_aws_endpoint = "http://integration-localstack-1:4566"

    # Optional ARN to use on the stream (default: "")
    role_arn    = "arn:aws:iam::123456789012:role/myrole"
  }
}

