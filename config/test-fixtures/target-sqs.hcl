# config for sqs target

target {
  use "sqs" {
    queue_name = "testQueue"
    region     = "eu-test-1"
    role_arn   = "xxx-test-role-arn"
  }
}
