# transform configuration - js - simple

engine {
  use "js" {
    name = "test-engine"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICByZXR1cm4geDsKfQkgICAKCQ=="
  }
}

transform {
  use "js" {
    engine_name="test-engine"
  }
}
