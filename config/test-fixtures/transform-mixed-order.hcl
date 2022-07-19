transform {
  use "js" {
    // changes app_id to "1"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gIjEiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}

transform {
  use "js" {
    // if app_id == "1" it is changed to "2"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGlmIChqc29uT2JqWyJhcHBfaWQiXSA9PSAiMSIpIHsKICAgICAgICBqc29uT2JqWyJhcHBfaWQiXSA9ICIyIgogICAgfQogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}

transform {
  use "js" {
    // if app_id == "2" it is changed to "3"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGlmIChqc29uT2JqWyJhcHBfaWQiXSA9PSAiMiIpIHsKICAgICAgICBqc29uT2JqWyJhcHBfaWQiXSA9ICIzIgogICAgfQogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}