transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^test-data1$"
  }
}

disable_telemetry = true