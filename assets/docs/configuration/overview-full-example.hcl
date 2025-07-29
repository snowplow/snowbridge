source {
  use "kinesis" {
    # Kinesis stream name to read from (required)
    stream_name       = "my-stream"

    # AWS region of Kinesis stream (required)
    region            = "us-west-1"

    # App name for Snowbridge (required)
    app_name          = "SnowbridgeProd1"

    # Optional ARN to use on source stream (default: "")
    role_arn          = "arn:aws:iam::123456789012:role/myrole"

    # Timestamp for the kinesis shard iterator to begin processing.
    # Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
    # (default: TRIM_HORIZON)
    start_timestamp   = "2020-01-01 10:00:00"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 15
  }
}

transform {
  use "spEnrichedFilter" {
    # keep only page views
    atomic_field = "event_name"

    regex = "^page_view$"

    filter_action = "keep"
  }
}

transform {
  use "js" {
    # We use an env var here to facilitate tests. A hardcoded path will also work.
    script_path = env.JS_SCRIPT_PATH
  }
}

target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}

failure_target {
    use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}

filter_target {
    use "http" {
      url = "https://test-server"
  }
}

sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://1234d@sentry.acme.net/28"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}

stats_receiver {
  use "statsd" {
    # StatsD server address
    address = "127.0.0.1:8125"

    # StatsD metric prefix (default: "snowplow.snowbridge")
    prefix  = "snowplow.snowbridge"

    # Escaped JSON string with tags to send to StatsD (default: "{}")
    tags    = "{\"aKey\": \"aValue\"}"
  }

  # Time (seconds) the observer waits for new results (default: 1)
  timeout_sec = 2

  # Aggregation time window (seconds) for metrics being collected (default: 15)
  buffer_sec  = 20
}

# log level configuration (default: "info")
log_level = "info"

# Specifies how failed writes to the target should be retried, depending on an error type 
retry {
  transient {
    # Initial delay (before first retry) for transient errors
    delay_ms = 1000 

    # Maximum number of retries for transient errors
    max_attempts = 5 
  }
  setup {
    # Initial delay (before first retry) for setup errors
    delay_ms = 20000
  }
}

metrics {
  # Optional toggle for E2E latency (difference between Snowplow collector timestamp and target write timestamp)
  enable_e2e_latency = true
}

monitoring {
  webhook {
    # An actual HTTP endpoint where monitoring events would be sent
    endpoint = "https://webhook.acme.com"

    # Set of arbitrary key-value pairs attached to the payload
    tags = {
      pipeline = "production"
    }

    # How often to send the heartbeat event
    heartbeat_interval_seconds = 3600
  }

  metadata_reporter {
    # An actual HTTP endpoint where metadata events would be sent
    endpoint = "https://webhook.metadata.com"

    # Set of arbitrary key-value pairs attached to the payload
    tags = {
      pipeline = "production"
    }
  }
}

license {
  accept = true
}
