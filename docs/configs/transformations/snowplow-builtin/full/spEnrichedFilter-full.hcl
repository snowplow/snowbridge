transform {
  use "spEnrichedFilter" {
    atomic_field = "platform"
    regex = "web|mobile"
    regex_timeout = 10
  }
}