transform {
  use "spEnrichedFilterUnstructEvent" {
    # Event name for custom event - this will match against the `event_name` field 
    unstruct_event_name = "add_to_cart"

    # Path to the field to filter on, within the custom event
    custom_field_path = "sku"

    # Regex pattern to match against. Matches will be kept
    regex = "test-data"

    # Regex for the schema version to match. Events whose verison doesn't match this regex will be filtered out.
    unstruct_event_version_regex = "1-*-*"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns are less performant
    regex_timeout = 10
  }
}
