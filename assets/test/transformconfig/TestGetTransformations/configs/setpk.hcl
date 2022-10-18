transform {
  use "spEnrichedSetPk" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "app_id"
  }
}
