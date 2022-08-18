transform {
  use "spEnrichedFilter" {
    atomic_field = "platform"
    regex = "web|mobile"
  }
}