# Monitoring Configuration

## Stats and metrics

Stream Replicator comes with configurable logging, [pprof](https://github.com/google/pprof) profiling, [statsD](https://www.datadoghq.com/statsd-monitoring) statistics and [Sentry](https://sentry.io/welcome/) integrations to ensure that you know what’s going on.

### Logging

Use the log_level parameter to specify the log level.

```hcl
// log level configuration (default: "info")
log_level = "debug"
```

### Sentry Configuration

```hcl
sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://1234d@sentry.acme.net/28"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}
```
### StatsD stats reciever 

```hcl
stats_receiver {
  use "statsd" {
    # StatsD server address
    address = "127.0.0.1:8125"

    # StatsD metric prefix (default: "snowplow.stream-replicator")
    prefix  = "snowplow.stream-replicator"

    # Escaped JSON string with tags to send to StatsD (default: "{}")
    tags    = "{\"aKey\": \"aValue\"}"
  }

  # Time (seconds) the observer waits for new results (default: 1)
  timeout_sec = 2

  # Aggregation time window (seconds) for metrics being collected (default: 15)
  buffer_sec  = 20
}
```
