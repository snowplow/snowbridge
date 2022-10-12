transform {
  use "js" {
    timeout_sec = 15
    // return x;
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICByZXR1cm4geDsKfQ=="
  }
}

transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "wrong"
    filter_action = "keep"
  }
}

transform {
  use "js" {
    timeout_sec = 15
    // return x;
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICByZXR1cm4geDsKfQ=="
  }
}