transform {
  use "jqFilter" {
    # Full JQ command which will be used to filter input data. The output must be boolean. If 'false', data is then discarded.
    jq_command = "has(\"app_id\")"
  }
}
