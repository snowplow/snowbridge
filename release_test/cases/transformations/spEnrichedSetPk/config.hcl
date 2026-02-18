transform {
  use "spEnrichedSetPk" {
    atomic_field = "event_id"
  }
  worker_pool = 1
}

disable_telemetry = true