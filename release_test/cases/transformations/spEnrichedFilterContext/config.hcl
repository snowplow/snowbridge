transform {
  use "spEnrichedFilterContext" {

    context_full_name = "contexts_com_acme_just_ints_1"
    custom_field_path = "integerField"
    regex = "^0$"
  }
}

disable_telemetry = true