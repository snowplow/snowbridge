transform {
  use "js" {
    timeout_sec = 15
    // return x;
    script_path = env.JS_PASSTHROUGH_PATH
  }

  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "wrong"
    filter_action = "keep"
  }

  use "js" {
    timeout_sec = 15
    // return x;
    script_path = env.JS_PASSTHROUGH_PATH
  }
}