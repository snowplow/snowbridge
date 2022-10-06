# Extended configuration for SQS as a source (all options)

source {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"

    # Role ARN to use on source queue
    role_arn   = "arn:aws:iam::123456789012:role/myrole"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20
  }
}