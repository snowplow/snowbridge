target {
  use "kinesis" {

    stream_name = "e2eKinesisTarget"

    region      = "us-east-1"
    
    custom_aws_edpoint = "http://integration-localstack-1:4566"
  }
}