transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgIGlmIHN0cmluZy5maW5kKGlucHV0LkRhdGEsICJhaWRfNiIsIDAsIHRydWUpIH49IG5pbCB0aGVuCiAgICAgICAgcmV0dXJuIGlucHV0CiAgICBlbHNlCiAgICAgICAgcmV0dXJuIHtEYXRhID0gIiIsIEZpbHRlck91dCA9IHRydWV9CiAgICBlbmQKZW5kCg=="

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}

disable_telemetry = true