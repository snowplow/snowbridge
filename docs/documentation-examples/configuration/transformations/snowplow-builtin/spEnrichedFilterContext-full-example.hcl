transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_com_acme_env_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "environment"

    # Regex pattern to match against. Matches will be kept
    regex = "^prod$"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns are less performant
    regex_timeout = 10
  }
}
