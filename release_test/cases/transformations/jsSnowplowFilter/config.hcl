transform {
  use "js" {
    script_path = "/script.js"
    
    snowplow_mode       = true 
  }
  worker_pool = 1
}

disable_telemetry = true