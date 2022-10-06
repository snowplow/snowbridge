transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "platform"

    # Regex pattern to match against. Matches will be kept
    regex = "web|mobile"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns are less performant
    regex_timeout = 10
  }
}
