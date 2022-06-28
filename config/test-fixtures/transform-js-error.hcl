# transform configuration - js - compile error

engine {
  use "js" {
    name = "test-engine"
    source_b64 = "ZnVuY3Rpb24ganNvblRyYW5zZm9ybUZpZWxkTmFtZU9iaih4KSB7CgogICAgdmFyIGpzb25PYmogPSBKU09OLnBhcnNlKHguRGF0YSk7CiAgICBqc29uT2JqWyJ3cm9uZ19rZXkiXSA9ICJ3aHkgYXJlIHlvdSBkZWNvZGluZyB0aGlzIjsKCiAgICByZXR1cm4gewogICAgICAgIERhdGE6IEpTT04uc3RyaW5naWZ5KGpzb25PYmopCiAgICB9Owp9"
  }
}

transform {
  use "js" {
    engine_name="test-engine"
  }
}
