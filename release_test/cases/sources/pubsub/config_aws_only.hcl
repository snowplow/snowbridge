source {
  use "pubsub" {
    project_id      = "project-test"
    subscription_id = "e2e-pubsub-source-subscription-aws-only"
  }
}

transform {
  worker_pool = 1
}

disable_telemetry = true