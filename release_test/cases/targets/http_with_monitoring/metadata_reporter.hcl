# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
}

target {
    use "http" {
        url = "http://host.docker.internal:12080/target"

        request_timeout_in_millis = 3 // manufacture timeouts

        response_rules {
            rule {
                type = "invalid"
                http_codes = [0] // client error
            }
            rule {
                type = "setup"
                http_codes =  [401, 403]
            }
        }
    }
}

failure_target {
    use "http" {
        url = "http://host.docker.internal:12080/invalid"
    }
}

stats_receiver {
  # This determines the flushing behaviour for metadata reporter too
  buffer_sec = 1
  timeout_sec = 1
}

monitoring {
  metadata_reporter {
    # An actual HTTP endpoint where metadata events would be sent
    endpoint = "http://host.docker.internal:12080/metadata"
  }
}

disable_telemetry = true