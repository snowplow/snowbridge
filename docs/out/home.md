Easily replicate data streams wherever you need them to be!  This application is available as a standalone CLI based application.

![Stream Replicator](https://user-images.githubusercontent.com/40794354/183377471-fc84580a-1b74-41b4-b42d-33e5a7b4b58f.jpg)

# Configuration

- [Logging, Observability and Sentry](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Logging,-Observability-and-Sentry)
- [Sources](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Sources)
- [Transformations and Filters](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Transformations-and-Filters)
- [Targets](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Targets)
- [Failure](https://github.com/snowplow-devops/stream-replicator/wiki/Config:-Failure-Targets-&-Formats)

# Configuration examples:

## HCL configuration

Stream Replicator can be configured using a HCL file. The location of this file is defined in the `STREAM_REPLICATOR_CONFIG_FILE` environment variable.

`export STREAM_REPLICATOR_CONFIG_FILE="/Users/example_user/conf.hcl"`

```hcl
source {
  use "kinesis" {
    # Kinesis stream name to read from (required)
    stream_name       = "my-stream"

    # AWS region of Kinesis stream (required)
    region            = "us-west-1"

    # App name for Stream Replicator (required)
    app_name          = "StreamReplicatorProd1"

    # Optional ARN to use on source stream (default: "")
    role_arn          = "arn:aws:iam::123456789012:role/myrole"

    # Timestamp for the kinesis shard iterator to begin processing.
    # Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
    # (default: TRIM_HORIZON)
    start_timestamp   = "2020-01-01 10:00:00"

    # Number of events to process concurrently (default: 50)
    concurrent_writes = 15
  }
}

transform {
  use "spEnrichedFilter" {
    # keep only page views
    atomic_field = "event_name"
    regex = "^page_view$"
  }
}

transform {
  use "js" {
    # changes app_id to "1"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gIjEiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
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

// log level configuration (default: "info")
log_level = "info"

sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://acme.com/1"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}

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

disable_telemetry = true
user_provided_id = "hello-this-is-us"

```

## Environment variables

If `STREAM_REPLICATOR_CONFIG_FILE` is not set, stream replicator will instead look to environment variables for configuration.

The source, target and failure target type is indicated using `SOURCE_NAME`, `TARGET_NAME` and `FAILURE_TARGET_NAME`. Transformations are configured by populating the `TRANSFORM_CONFIG_B64` environment variable with a base-64 encoded hcl block.

The above configuration, using environment variables is:

```bash
#!/bin/sh

# source
export SOURCE_NAME="kinesis"                                    \
SOURCE_KINESIS_STREAM_NAME="my-stream"                          \
SOURCE_KINESIS_REGION="us-west-1"                               \
SOURCE_KINESIS_APP_NAME="StreamReplicatorProd1"                 \
SOURCE_KINESIS_ROLE_ARN="arn:aws:iam::123456789012:role/myrole" \
SOURCE_KINESIS_START_TSTAMP="2020-01-01 10:00:00"               \
SOURCE_CONCURRENT_WRITES=15

# transformations
export TRANSFORM_CONFIG_B64="dHJhbnNmb3JtIHsKICB1c2UgInNwRW5yaWNoZWRGaWx0ZXIiIHsKICAgICMga2VlcCBvbmx5IHBhZ2Ugdmlld3MKICAgIGF0b21pY19maWVsZCA9ICJldmVudF9uYW1lIgogICAgcmVnZXggPSAiXnBhZ2VfdmlldyQiCiAgfQp9Cgp0cmFuc2Zvcm0gewogIHVzZSAianMiIHsKICAgICMgY2hhbmdlcyBhcHBfaWQgdG8gIjEiCiAgICBzb3VyY2VfYjY0ID0gIlpuVnVZM1JwYjI0Z2JXRnBiaWg0S1NCN0NpQWdJQ0IyWVhJZ2FuTnZiazlpYWlBOUlFcFRUMDR1Y0dGeWMyVW9lQzVFWVhSaEtUc0tJQ0FnSUdwemIyNVBZbXBiSW1Gd2NGOXBaQ0pkSUQwZ0lqRWlPd29nSUNBZ2NtVjBkWEp1SUhzS0lDQWdJQ0FnSUNCRVlYUmhPaUJLVTA5T0xuTjBjbWx1WjJsbWVTaHFjMjl1VDJKcUtRb2dJQ0FnZlRzS2ZRPT0iCiAgfQp9"

# target
export TARGET_NAME="pubsub"                 \
TARGET_PUBSUB_PROJECT_ID="acme-project"     \
TARGET_PUBSUB_TOPIC_NAME="some-acme-topic"

# logging
export LOG_LEVEL="debug"

# reporting and stats
export SENTRY_DSN="https://acme.com/1"    \
SENTRY_DEBUG=true                         \
SENTRY_TAGS="{\"aKey\":\"aValue\"}"

export STATS_RECEIVER_NAME="statsd"                       \
STATS_RECEIVER_STATSD_ADDRESS="127.0.0.1:8125"            \
STATS_RECEIVER_STATSD_PREFIX="snowplow.stream-replicator" \
STATS_RECEIVER_TIMEOUT_SEC=2                              \
STATS_RECEIVER_BUFFER_SEC=20

export DISABLE_TELEMETRY=false         \
USER_PROVIDED_ID="elmer.fudd@acme.com"
```

# Runtimes

- [CLI (Standalone)](https://github.com/snowplow-devops/stream-replicator/wiki/Runtime:-CLI-(Standalone))

# Profiling

- [Using pprof](https://github.com/snowplow-devops/stream-replicator/wiki/Profiling-with-pprof)

# Contributing

- [Adding targets](https://github.com/snowplow-devops/stream-replicator/wiki/Contributing:-How-to-add-a-new-%60target%60%3F)
