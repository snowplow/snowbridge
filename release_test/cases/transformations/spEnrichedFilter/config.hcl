transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^aid_6"
    filter_action = "keep"
  }
  worker_pool = 1
}

disable_telemetry = true