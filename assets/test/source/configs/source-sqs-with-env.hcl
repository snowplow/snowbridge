# sqs source configuration

source {
  use "sqs" {
    queue_name = env.TEST_SQS_QUEUE_NAME
    region     = "us-test-1"
  }
}
