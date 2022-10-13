transform {
  use "js" {
    timeout_sec = 15
    // return x;
    script_path = env.JS_PASSTHROUGH_PATH
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
    script_path = env.JS_PASSTHROUGH_PATH
  }
}