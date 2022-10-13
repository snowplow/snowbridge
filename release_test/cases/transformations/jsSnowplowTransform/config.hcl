transform {
  use "js" {
    script_path = "/script.js"
    
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}

disable_telemetry = true