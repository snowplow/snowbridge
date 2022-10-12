transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^aid_6"
    filter_action = "keep"
  }
}

disable_telemetry = true