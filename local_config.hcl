source {
  use "eventHub" {
    # Azure EventHub hub name to read from (required)
    namespace = "mikhail-snowplow-namespace.servicebus.windows.net"
    name      = "enriched-topic"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 1
  }
}

transform {
  use "spEnrichedToJson" {
  }
}

target {
  use "stdout" {
    data_only_output = true
  }
}

# log level configuration (default: "info")
log_level = "debug"

license {
  accept = true
}
