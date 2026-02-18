source {
  use "sqs" {
    queue_name = "sqs-queue-e2e-source"
    region     = "us-east-1"
    custom_aws_endpoint = "http://integration-localstack-1:4566"
  }
}

target {
  use "stdout" {
    batching {
      max_batch_messages = 10
    }
  }
}

transform {
  worker_pool = 5
}

disable_telemetry = true