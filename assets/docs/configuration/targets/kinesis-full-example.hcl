# Extended configuration for Kinesis as a target (all options)

target {
  use "kinesis" {
    # Kinesis stream name to send data to
    stream_name = "my-stream"

    # AWS region of Kinesis stream
    region      = "us-west-1"

    # Optional custom endpoint url to override aws endpoints,
    # this is for use with local testing tools like localstack - don't set for production use.
    custom_aws_endpoint = "http://integration-localstack-1:4566"

    # Optional ARN to use on the stream (default: "")
    role_arn    = "arn:aws:iam::123456789012:role/myrole"

    # Set the maximum amount of messages that can be in one request
    # The maximum value for a PutRecords request is 500. If set to higher, 500 will be used.
    # If a single message in a request fails, all messages will be retried. This configuration can be used to manage risk of duplicates
    request_max_messages = 1
  }
}

