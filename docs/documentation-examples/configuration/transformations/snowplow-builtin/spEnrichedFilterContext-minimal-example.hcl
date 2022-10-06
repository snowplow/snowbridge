transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_com_acme_env_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "environment"

    # Regex pattern to match against. Matches will be kept
    regex = "^prod$"
  }
}
