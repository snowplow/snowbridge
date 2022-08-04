# kinesis source extended configuration

source {
  use "kinesis" {
    stream_name                     = "testStream"
    region                          = "us-test-1"
    role_arn                        = "xxx-test-role-arn"
    app_name                        = "testApp"
    start_timestamp                 = "2022-03-15 07:52:53"
    concurrent_writes               = 51
    shard_check_frequency_seconds   = 20
    client_record_max_age_seconds   = 30
    leader_action_frequency_seconds = 25
  }
}
