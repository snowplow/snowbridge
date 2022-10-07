transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KQoJbG9jYWwganNvbiA9IHJlcXVpcmUoImpzb24iKQoJbG9jYWwganNvbk9iaiwgXyA9IGpzb24uZGVjb2RlKHguRGF0YSkKCWlmIGpzb25PYmouYmF0bW9iaWxlQ291bnQgPCAxIHRoZW4gCgkJcmV0dXJuIHtEYXRhID0gIiIsIEZpbHRlck91dCA9IHRydWV9CgllbmQKCWpzb25PYmoubmFtZSA9ICJCcnVjZSBXYXluZSIKCXJldHVybiB7IERhdGEgPSBqc29uT2JqLCBQYXJ0aXRpb25LZXkgPSBqc29uT2JqLmlkIH0KICBlbmQ="

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}