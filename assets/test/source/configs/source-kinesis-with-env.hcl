# kinesis source with env vars to be used for testing

source {
  use "kinesis" {
    stream_name       = env.TEST_KINESIS_STREAM_NAME
    region            = env.TEST_KINESIS_REGION
    app_name          = env.TEST_KINESIS_APP_NAME
  }
}
