transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgIG5ldyA9IHN0cmluZy5nc3ViKGlucHV0LkRhdGEsICJ0ZXN0JS1kYXRhIiwgInRlc3QiKQogICAgcmV0dXJuIHtEYXRhID0gbmV3fQplbmQ="

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}