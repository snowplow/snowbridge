
target {
	use "pubsub" {

		project_id = "project-test"

		topic_name = "e2e-target-topic"
	}
}

transform {
  worker_pool = 1
}

disable_telemetry = true