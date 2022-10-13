# transform configuration - js - extended

transform {
  use "js" {
    script_path = env.JS_PARSE_JSON_PATH
    timeout_sec         = 20
    disable_source_maps = true
    snowplow_mode       = false
  }
}