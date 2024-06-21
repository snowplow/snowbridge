source {
  use "kafka" {
    brokers = env.TEST_KAFKA_BROKERS
    topic_name = env.TEST_KAFKA_TOPIC_NAME
    consumer_name = env.TEST_KAFKA_CONSUMER_NAME
    offsets_initial = env.TEST_KAFKA_OFFSETS_INITIAL
  }
}
