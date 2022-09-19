transform {
  use "spEnrichedFilterContext" {

    context_full_name = "contexts_com_acme_just_ints_1"
    custom_field_path = "integerField"
    regex = "^0$"
  }
}

# log_level = "error" # we set log level to error to minimise noise at output - turns out we don't need to (for now at least) - `>` in our script won't put logs in the file.