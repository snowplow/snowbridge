# Extended configuration for SQS as a target (all options)

target {
  use "sqs" {
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
