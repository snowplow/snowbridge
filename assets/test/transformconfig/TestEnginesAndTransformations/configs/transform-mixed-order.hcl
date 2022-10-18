transform {
  use "js" {
    // changes app_id to "1"
    script_path = env.JS_ORDER_TEST_1
  }
}

transform {
  use "js" {
    // if app_id == "1" it is changed to "2"
    script_path = env.JS_ORDER_TEST_2
  }
}

transform {
  use "js" {
    // if app_id == "2" it is changed to "3"
    script_path = env.JS_ORDER_TEST_3
  }
}