source {
  use "sqs" {
    queue_name = "sqs-queue-e2e-source"
    region     = "us-east-1"
    custom_aws_edpoint = "http://integration-localstack-1:4566"
  }
}

disable_telemetry = true