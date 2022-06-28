# transform configuration - lua - no source code

engine {
  use "lua" {
    name = "test-engine"
  }
}

transform {
  use "lua" {
    engine_name="test-engine"
  }
}
