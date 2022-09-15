# Configuration Overview

Stream Replicator cis configured using [HCL](https://github.com/hashicorp/hcl). To configure Stream Replicator, create your configuration in a file with `.hcl` extension, and set the `STREAM_REPLICATOR_CONFIG_FILE` environment variable to the path to your file.

For any option, you can reference an environment variable using the `env` object. So, to refer to an environment variable named `MY_ENV_VAR` in your configuration, you can use  `env.MY_ENV_VAR`. It is recommended that you use enviornment variables for any sensitive value, such as a password.

For most options, stream replicator uses blocks for configuration. The `use` keyword specifies what you'd like to configure - for example a kinesis source is configured using `source { use "kinesis" {...}}`.

For all configuration blocks except for transformations, you must provide only one block (or none, to use the defaults).

For transformations, you may provide 0 or more `transform` configuration blocks. All provided `transform` blocks will be applied to the data, one after another, in the order provided. The exception to this is when a filter is applied and the filter condition is met - in this case the message will be acked and subsequent transformations will not be applied (neither will the data be sent to the destination).

Some application-level options are not contained in a transformation block, rather are top-level options in the configuration. For example, to set the log level of the application, we just set the top-level variable `log_level`.

If you do not provide a configuration, or provide an empty one, the defaults of `stdin` source, no transformations, `stdout` target, and `stdout` failure target will be used. No external statistics reporting or sentry error reporting will be used.

The below example is a complete configuration, which configures a kinesis source, a builtin Snowplow filter (which may only be used if the input is Snowplow enriched data), a custom javascript transformation, and a pubsub target, as well as the statsD stats receiver, and sentry for error reporting.

In layman's terms, this configuration will read data from a kinesis stream, filter out any data whose `event_name` field is not `page_view`, run a custom Javascript script upon the data to change the app_id to `"1"`, and send the transformed page view data to pubsub. It will also send statistics about what it's doing to a statsD endpoint, and will send information about errors to a sentry endpoint.

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

    # Maximum concurrent processes for the app (default: 50)
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

// log level configuration (default: "info")
log_level = "info"
disable_telemetry = true
user_provided_id = "hello-this-is-us"
```