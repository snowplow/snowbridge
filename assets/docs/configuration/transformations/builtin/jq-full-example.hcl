transform {
  use "jq" {
    # Full JQ command which will be used to transform input data.  
    jq_command = <<JQEOT
{
    my_app_id: .app_id,
    my_nested_prop: {
        playback_rate: .contexts_com_snowplowanalytics_snowplow_media_player_1[0].playbackRate
    }
}
JQEOT

    # Optional. Timeout for execution of the script, in milliseconds.
    timeout_ms = 800

    # Optional, may be used when the input is a Snowplow enriched TSV. 
    # This will transform the data so that the root '.' JQ field contains JSON object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode = true
  }
}
