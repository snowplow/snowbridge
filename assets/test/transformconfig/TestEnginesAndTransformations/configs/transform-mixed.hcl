transform {
  use "js" {
    script_path = env.JS_ALTER_AID_1_PATH
  }
}

transform {
  use "js" {
    script_path = env.JS_ALTER_AID_2_PATH
  }
}

transform {
  use "lua" {
    script_path = env.LUA_ADD_HELLO_PATH
  }
}