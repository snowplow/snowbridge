# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
  worker_pool = 1
}

target {
  use "http" {
    batching {
      max_batch_messages = 1
      max_concurrent_batches = 50
    }
    url = "http://host.docker.internal:9999/event"

    response_rules {
      rule {
          type = "setup"
          http_codes =  [401, 403]
        }
    }
  }
}

retry {
  setup {
    # Initial delay (before first retry) for setup errors
    delay_ms = 200
  }
}

monitoring {
  webhook {
    # An actual HTTP endpoint where monitoring events would be sent
    endpoint = "http://host.docker.internal:9999/alert-heartbeat-monitoring"

    # Set of arbitrary key-value pairs attached to the payload
    tags = {
      pipeline = "release_tests"
    }

    # How often to send the heartbeat event
    heartbeat_interval_seconds = 1
  }
}

disable_telemetry = true