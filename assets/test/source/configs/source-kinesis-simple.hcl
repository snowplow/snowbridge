# kinesis source required configuration

source {
  use "kinesis" {
    stream_name = "testStream"
    region      = "us-test-1"
    app_name    = "testApp"
  }
}
