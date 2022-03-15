# kinesis target configuration

target {
  use "kinesis" {
    stream_name = "testStream"
    region      = "eu-test-1"
    role_arn    = "xxx-test-role-arn"
  }
}
