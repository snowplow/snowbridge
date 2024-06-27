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

disable_telemetry = true