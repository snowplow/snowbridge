transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^aid_6"
  }
}

disable_telemetry = true