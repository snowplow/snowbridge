# Pulsar target required configuration

target {
  use "pulsar" {
    broker_service_url    = "pulsar://test:6650"
    topic_name            = "testTopic"
  }
}
