# Extended configuration for PubSub as a target (all options)

target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"

    # Optional: Path to service account JSON credentials file
    # If not provided, uses Google Application Default Credentials
    credentials_json_path = "/path/to/service-account.json"
  }
}
