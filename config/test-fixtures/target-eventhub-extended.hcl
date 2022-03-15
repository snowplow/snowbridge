# eventhub target extended config

target {
  use "eventhub" {
    namespace                  = "testNamespace"
    name                       = "testName"
    max_auto_retries           = 2
    message_byte_limit         = 1000000
    chunk_byte_limit           = 1000000
    chunk_message_limit        = 501
    context_timeout_in_seconds = 21
    batch_byte_limit           = 1000000
  }
}
