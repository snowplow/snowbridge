# HTTP target default behaviour requires JSON
transform {
  use "spEnrichedToJson" {
  }
}

target {
    use "http" {
        url = "http://host.docker.internal:12080/target"

        response_rules {
            rule {
                type = "invalid"
                http_codes = [400]
            }
            rule {
                type = "setup"
                http_codes =  [401, 403]
            }
        }
    }
}

monitoring {
  metadata_reporter {
    # An actual HTTP endpoint where metadata events would be sent
    endpoint = "http://host.docker.internal:12080/metadata"
  }
}

disable_telemetry = true