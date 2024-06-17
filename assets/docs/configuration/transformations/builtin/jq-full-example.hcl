transform {
  use "jq" {
    jq_command = <<JQEOT
{
    my_api_key: "${env.TESTAPIKEY}",
    my_app_id: .app_id,
    my_nested_prop: {
        playback_rate: .contexts_com_snowplowanalytics_snowplow_media_player_1[0].playbackRate
    }
}
JQEOT

    timeout_sec = 5
    snowplow_mode = true
  }
}
