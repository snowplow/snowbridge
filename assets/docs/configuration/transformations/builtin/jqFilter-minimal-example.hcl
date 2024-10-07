transform {
  use "jqFilter" {
    jq_command = "has(\"app_id\")"
  }
}
