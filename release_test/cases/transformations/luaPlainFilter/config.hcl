transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgIGlmIHN0cmluZy5maW5kKGlucHV0LkRhdGEsICJ0ZXN0LWRhdGExIiwgMCwgdHJ1ZSkgfj0gbmlsIHRoZW4KICAgICAgICByZXR1cm4gaW5wdXQKICAgIGVsc2UKICAgICAgICByZXR1cm4ge0RhdGEgPSAiIiwgRmlsdGVyT3V0ID0gdHJ1ZX0KICAgIGVuZAplbmQK"

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}

disable_telemetry = true