# configuration with various invalid options

message_transformation = "fakeHCL"

target {
  use "fakeHCL" {}
}

failure_target {
  use "fakeHCL" {}
}
