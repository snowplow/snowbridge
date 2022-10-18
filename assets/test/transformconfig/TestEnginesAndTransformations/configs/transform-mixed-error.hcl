transform {
  use "js" {
    timeout_sec = 15
    script_path = env.JS_ALTER_AID_1_PATH
  }
}

transform {
  use "js" {
    timeout_sec = 15
    script_path = env.JS_ERROR_PATH
  }
}

transform {
  use "lua" {
    timeout_sec = 15
    script_path = env.LUA_ADD_HELLO_PATH
  }
}