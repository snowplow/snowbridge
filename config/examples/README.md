# Configuring stream-replicator from a file

Another option to configuring stream-replicator purely from environment variables as described in the [wiki](https://github.com/snowplow-devops/stream-replicator/wiki), is through an HCL file.

This directory contains particular examples of configuration options.

## General structure

The general structure of the configuration file is composed of:

1. Source configuration
2. Target configuration
3. Failure target configuration
4. Observability configuration
5. Transformation configuration
6. Additional configuration options

As a vague example:

```hcl
// block for configuring the source
source {
  use "sqs" {
    // block for configuring SQS as source
  }
}

// block for configuring the target
target {
  use "kafka" {
    // block for configuring Kafka as target
  }
}

// block for configuring the failure target
failure_target {
  use "pubsub" {
    // block for configuring PubSub as failure target
  }
}

// block for configuring stats receiver
stats_receiver {
  use "statsd" {
    // block for configuring StatsD as a receiver
  }
}

// block for configuring sentry
sentry {}

// string to configure message transformation (default: "none")
message_transformation = "none"

// log level configuration (default: "info")
log_level = "info"

// Ability to provide a GCP service account (b64) to the application directly
google_application_credentials = ""
```

So, a complete example could be:

```hcl
// example.hcl

source {
  use "sqs" {
    queue_name = "mySqsQueue"
    region     = "eu-west-1"
  }
}

target {
  use "kafka" {
    brokers    = "my-kafka-broker-connectinon-string"
    topic_name = "snowplow-enriched-good"
  }
}

failure_target {
  use "kinesis" {
    stream_name = "some-acme-stream"
    region      = "us-east-1"
  }
}

stats_receiver {
  use "statsd" {
    address = "127.0.0.1:8125"
  }
}

sentry {
  dsn   = "https://acme.com/1"
  debug = true
}

log_level = "debug"
```

In the example files in this directory, there is a simple and extended version for configuring each:

 - source
 - target
 - failure_target
 - sentry
 - stats-receiver

## Referencing environment variables in the configuration file

There are 2 ways to reference environment variables in the HCL file:

1. As `env("MY_ENV_VAR")`

    For example:

    ```txt
    sasl_password = env("SASL_PASSWORD")
    ```

2. As `env.MY_ENV_VAR`

    For example:

    ```txt
    sasl_password = env.SASL_PASSWORD
    ```
