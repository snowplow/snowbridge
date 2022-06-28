# transform configuration - js - extended

engine {
  use "js" {
    name = "test-engine"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gImNoYW5nZWQiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
    timeout_sec         = 20
    disable_source_maps = true
    snowplow_mode       = false
  }
}

transform {
  use "js" {
    engine_name="test-engine"
  }
}