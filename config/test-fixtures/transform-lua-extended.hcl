# transform configuration - lua - extended

transform {
  message_transformation = "lua:fun"

  use "lua" {
    source_b64  = "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ"
    timeout_sec = 10
    sandbox     = false
  }
}
