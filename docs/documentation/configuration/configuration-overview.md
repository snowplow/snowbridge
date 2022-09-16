# Configuration Overview

Stream Replicator is configured using [HCL](https://github.com/hashicorp/hcl). To configure Stream Replicator, create your configuration in a file with `.hcl` extension, and set the `STREAM_REPLICATOR_CONFIG_FILE` environment variable to the path to your file.

Inside the configuration, you can reference environment variables using the `env` object. For example, to refer to an environment variable named `MY_ENV_VAR` in your configuration, you can use  `env.MY_ENV_VAR`. We recommend employing environment variables for any sensitive value, such as a password, as opposed to adding the value to the configuration verbatim.

For most options, Stream Replicator uses blocks for configuration. The `use` keyword specifies what you'd like to configure - for example a kinesis source is configured using `source { use "kinesis" {...}}`.

For all configuration blocks except for transformations, you must provide only one block (or none, to use the defaults).

For transformations, you may provide 0 or more `transform` configuration blocks. They will be applied to the data, one after another, in the order they appear in the configuration. The exception to this is when a filter is applied and the filter condition is met - in this case the message will be acked and subsequent transformations will not be applied (neither will the data be sent to the destination).

Some application-level options are not contained in a block, instead they're top-level options in the configuration. For example, to set the log level of the application, we just set the top-level variable `log_level`.

If you do not provide a configuration, or provide an empty one, the application will use the defaults:
* `stdin` source;
* no transformations;
* `stdout` target;
* `stdout` failure target.
There’ll be no external statistics reporting or sentry error reporting.

The below example is a complete configuration, which specifies a kinesis source, a builtin Snowplow filter (which may only be used if the input is Snowplow enriched data), a custom javascript transformation, and a pubsub target, as well as the statsD stats receiver, and sentry for error reporting.

In layman's terms, this configuration will read data from a kinesis stream, filter out any data whose `event_name` field is not `page_view`, run a custom Javascript script upon the data to change the `app_id` to `"1"`, and send the transformed page view data to pubsub. It will also send statistics about what it's doing to a statsD endpoint, and will send information about errors to a sentry endpoint.

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

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
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
```