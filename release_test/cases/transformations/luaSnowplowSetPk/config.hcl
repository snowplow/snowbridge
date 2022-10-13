transform {
  use "lua" {
    script_path = "/script.lua"
    
    snowplow_mode       = true 
  }
}

disable_telemetry = true