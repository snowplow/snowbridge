transform {
  use "jqFilter" {
    jq_command = "has(\"app_id\")"
    timeout_ms = 800
    snowplow_mode = true
  }
}
