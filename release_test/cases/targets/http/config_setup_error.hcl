# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
}

target {
  use "http" {

    url = "http://host.docker.internal:11998/setup-error"

    response_rules {
      setup {
          http_codes =  [401]
        }
    }
  }
}

retry {
  setup {
    # Initial delay (before first retry) for setup errors
    delay_ms = 20
  }
}

disable_telemetry = true