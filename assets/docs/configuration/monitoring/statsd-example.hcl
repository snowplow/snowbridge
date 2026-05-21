stats_receiver {
  use "statsd" {
    # StatsD server address
    address = "127.0.0.1:8125"

    # StatsD metric prefix (default: "snowplow.snowbridge")
    prefix  = "snowplow.snowbridge"

    # Escaped JSON string with tags to send to StatsD (default: "{}")
    tags    = "{\"aKey\": \"aValue\"}"
  }

  # Aggregation time window (seconds) for metrics being collected (default: 60)
  buffer_sec  = 20
}