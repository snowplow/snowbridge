# kafka target required configuration

target {
  use "kafka" {
    brokers    = "testBrokers"
    topic_name = "testTopic"
  }
}
