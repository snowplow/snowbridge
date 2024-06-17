transform {
  use "jq" {
    jq_command = <<JQEOT
{
    my_app_id: .app_id,
    my_nested_prop: {
        playback_rate: .contexts_com_snowplowanalytics_snowplow_media_player_1[0].playbackRate
    }
}
JQEOT

    timeout_ms = 800
    snowplow_mode = true
  }
}
