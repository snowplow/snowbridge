transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgIG5ldyA9IHN0cmluZy5nc3ViKGlucHV0LkRhdGEsICJhaWRfIiwgInRlc3RfIikKICAgIHJldHVybiB7RGF0YSA9IG5ld30KZW5k"

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}

disable_telemetry = true