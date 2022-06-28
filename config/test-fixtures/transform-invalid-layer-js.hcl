# transform configuration - js - no source code

engine {
  use "js" {
    name = "test-engine"
  }
}

transform {
  use "js" {
    engine_name="test-engine"
  }
}