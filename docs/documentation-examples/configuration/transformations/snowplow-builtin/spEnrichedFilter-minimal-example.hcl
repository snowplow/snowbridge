transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "platform"

    # Regex pattern to match against. Matches will be kept
    regex = "web|mobile"
  }
}
