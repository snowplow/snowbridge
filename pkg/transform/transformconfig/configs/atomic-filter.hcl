transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "app_id"

    # Regex pattern to match against. Matches will be kept
    regex = "test-data1"

    filter_action = "keep"
  }
}

