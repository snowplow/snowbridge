transform {
  use "lua" {
    script_path = "/script.lua"
    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}

disable_telemetry = true