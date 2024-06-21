# configuration with various invalid options

target {
  use "fakeHCL" {}
}

failure_target {
  use "fakeHCL" {}
}

stats_receiver {
  use "fakeHCL" {}
}

log_level = "DEBUG"
