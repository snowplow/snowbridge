# transform configuration - js - extended

transform {
  message_transformation = "js:fun"

  use "js" {
    source_b64          = "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ"
    timeout_sec         = 10
    disable_source_maps = false
    snowplow_mode       = true
  }
}
