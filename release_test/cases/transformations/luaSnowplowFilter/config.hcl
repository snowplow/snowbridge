transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKCiAgICBhaWQgPSBpbnB1dC5EYXRhLmFwcF9pZAogICAgaWYgYWlkIH49IG5pbCBhbmQgc3RyaW5nLmZpbmQoIGFpZCwgImFpZF82IiwgMCwgdHJ1ZSkgfj0gbmlsIHRoZW4KCiAgICAgICAgcmV0dXJuIGlucHV0CiAgICBlbHNlCiAgICAgICAgcmV0dXJuIHtGaWx0ZXJPdXQgPSB0cnVlfQogICAgZW5kCmVuZA=="
    
    snowplow_mode       = true 
    sandbox = false
  }
}

disable_telemetry = true