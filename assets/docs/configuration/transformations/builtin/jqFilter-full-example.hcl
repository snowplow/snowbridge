transform {
  use "jqFilter" {
    # Full JQ command which will be used to filter input data. The output must be boolean. If 'false', data is then discarded.
    jq_command = "has(\"app_id\")"

    # Optional. Timeout for execution of the script, in milliseconds.
    timeout_ms = 800

    # Optional, may be used when the input is a Snowplow enriched TSV. 
    # This will transform the data so that the root '.' JQ  field contains JSON object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode = true
  }
}
