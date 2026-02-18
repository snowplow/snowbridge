target {
  use "kinesis" {

    stream_name = "e2eKinesisTarget"

    region      = "us-east-1"
    
    custom_aws_endpoint = "http://integration-localstack-1:4566"
  }
}

transform {
  worker_pool = 1
}

disable_telemetry = true