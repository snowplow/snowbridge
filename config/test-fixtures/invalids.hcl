# configuration with various invalid options

transform {
  message_transformation = "fakeHCL"
}

target {
  use "fakeHCL" {}
}

failure_target {
  use "fakeHCL" {}
}
