engine {
  use "js" {
    name = "engine1"
    timeout_sec = 15
    // return x;
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICByZXR1cm4geDsKfQ=="
  }
}

engine {
  use "js" {
    name = "engine2"
    timeout_sec = 15
    // return x;
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICByZXR1cm4geDsKfQ=="
  }
}

transform {
  use "js" {
    engine_name = "engine1"
  }
}

transform {
  use "spEnrichedFilter" {
    field = "app_id"
    regex = "wrong"
  }
}

transform {
  use "js" {
    engine_name = "engine2"
  }
}