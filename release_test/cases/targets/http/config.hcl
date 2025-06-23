# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
}


target {
  use "http" {

    url = "http://host.docker.internal:8998/e2e"
  }
}

monitoring {
  # An actual HTTP endpoint where monitoring events would be sent
  endpoint = "http://host.docker.internal:7997/data-monitoring"

  # Set of arbitrary key-value pairs attached to the payload
  tags = {
    pipeline = "release_tests"
  }

  # How often to send the heartbeat event (in seconds)
  heartbeat_interval = 1
}

disable_telemetry = true