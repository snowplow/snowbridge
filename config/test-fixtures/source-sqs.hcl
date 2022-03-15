# sqs source configuration

source {
  use "sqs" {
    queue_name = "testQueue"
    region     = "us-test-1"
    role_arn   = "xxx-test-role-arn"
  }
}
