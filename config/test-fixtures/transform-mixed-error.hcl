engine {
  use "js" {
    name = "engine1"
    timeout_sec = 15
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gImNoYW5nZWQiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}

engine {
  use "js" {
    name = "engine2"
    timeout_sec = 15
    source_b64 = "ZnVuY3Rpb24ganNvblRyYW5zZm9ybUZpZWxkTmFtZU9iaih4KSB7CgogICAgdmFyIGpzb25PYmogPSBKU09OLnBhcnNlKHguRGF0YSk7CiAgICBqc29uT2JqWyJ3cm9uZ19rZXkiXSA9ICJ3aHkgYXJlIHlvdSBkZWNvZGluZyB0aGlzIjsKCiAgICByZXR1cm4gewogICAgICAgIERhdGE6IEpTT04uc3RyaW5naWZ5KGpzb25PYmopCiAgICB9Owp9"
  }
}

engine {
  use "lua" {
    name = "engine3"
    timeout_sec = 15
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KQogICB4LkRhdGEgPSAiSGVsbG86IiAuLiB4LkRhdGEKICAgcmV0dXJuIHgKZW5k"
  }
}

transform {
  use "js" {
    engine_name = "engine1"
  }
}

transform {
  use "js" {
    engine_name = "engine2"
  }
}

transform {
  use "lua" {
    engine_name = "engine3"
  }
}