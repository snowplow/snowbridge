# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
}

target {
  use "http" {

    url = "http://host.docker.internal:7997/alert"

    response_rules {
      setup {
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
  # An actual HTTP endpoint where monitoring events would be sent
  endpoint = "http://host.docker.internal:7997/alert-monitoring"

  # Set of arbitrary key-value pairs attached to the payload
  tags = {
    pipeline = "release_tests"
  }

  # How often to send the heartbeat event
  heartbeat_interval_seconds = 1
}

disable_telemetry = true