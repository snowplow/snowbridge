transform {
  use "spEnrichedSetPk" {
    atomic_field = "app_id"
  }
}

# log_level = "error" # we set log level to error to minimise noise at output - turns out we don't need to (for now at least) - `>` in our script won't put logs in the file.