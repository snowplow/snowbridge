# Minimal configuration for SQS as a source (only required options)

source {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}