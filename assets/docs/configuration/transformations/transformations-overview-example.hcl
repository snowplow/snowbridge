transform {
  use "spEnrichedFilter" {
    # keep only page views

    atomic_field = "event_name"

    regex = "^page_view$"
    
    filter_action = "keep"
  }
}

transform {
  use "js" {
    # We use an env var here to facilitate tests. A hardcoded path will also work.
    script_path = env.JS_SCRIPT_PATH
  }
}