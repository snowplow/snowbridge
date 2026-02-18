# Extended configuration for SQS as a target (all options)

target {
  use "sqs" {
    batching {
      # Maximum number of events that can go into one batched request (default: 10)
      max_batch_messages     = 2
      # Maximum byte limit for a single batched request (default: 1048576)
      max_batch_bytes        = 1000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 1000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 500)
      flush_period_millis    = 200
    }

    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"

    # Optional custom endpoint url to override aws endpoints,
    # this is for use with local testing tools like localstack - don't set for production use.
    custom_aws_endpoint = "http://integration-localstack-1:4566"

    # Role ARN to use on SQS queue
    role_arn   = "arn:aws:iam::123456789012:role/myrole"
  }
}
