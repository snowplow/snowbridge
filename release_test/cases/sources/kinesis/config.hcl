source {
  use "kinesis" {

    stream_name = "e2eKinesisSource"

    region      = "us-east-1"

    app_name    = "e2eKinesisSource"

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