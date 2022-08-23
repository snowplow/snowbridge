source {
  use "kinesis" {

    stream_name = "e2eKinesisSource"

    region      = "us-east-1"

    app_name    = "e2eKinesisSource"

    custom_aws_edpoint = "http://integration-localstack-1:4566"
  }
}

disable_telemetry = true