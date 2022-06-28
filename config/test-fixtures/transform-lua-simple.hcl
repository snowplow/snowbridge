# transform configuration - lua - simple

engine {
  use "lua" {
    name = "test-engine"
    source_b64 = "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ"
  }
}

transform {
  use "lua" {
    engine_name="test-engine"
  }
}
