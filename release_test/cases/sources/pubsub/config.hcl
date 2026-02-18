source {
  use "pubsub" {
    project_id      = "project-test"
    subscription_id = "e2e-pubsub-source-subscription"
  }
}

target {
  use "stdout" {
    batching {
      max_batch_messages = 10
    }
  }
}

transform {
  worker_pool = 5
}

disable_telemetry = true