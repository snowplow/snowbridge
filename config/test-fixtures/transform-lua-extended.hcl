# transform configuration - lua - extended

engine {
  use "lua" {
    name = "test-engine"
    source_b64 = "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ"
    timeout_sec = 10
    snowplow_mode = false
    sandbox     = false
  }
}

transform {
  use "lua" {
    engine_name="test-engine"
  }
}