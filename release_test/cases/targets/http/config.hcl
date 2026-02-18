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
    }

    url = "http://host.docker.internal:8998/e2e"
  }
}

disable_telemetry = true