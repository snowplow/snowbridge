transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgICAgICBpbnB1dFsiUGFydGl0aW9uS2V5Il0gPSAidGVzdC1kYXRhMSIKICAgICAgICByZXR1cm4gaW5wdXQKZW5k"

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}

disable_telemetry = true