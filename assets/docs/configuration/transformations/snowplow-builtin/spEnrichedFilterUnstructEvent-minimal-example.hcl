transform {
  use "spEnrichedFilterUnstructEvent" {
    # Event name for custom event - this will match against the `event_name` field
    unstruct_event_name = "add_to_cart"

    # Path to the field to filter on, within the custom event
    custom_field_path = "sku"

    # Regex to match. Only matches against this regex are kept
    regex = "test-data"

    # Specifies the behaviour of the filter on a match:
    # "keep" continues to process the message to the target when the regex is matched, 
    # "drop" acks the message immediately and does not send it to the target.
    filter_action = "keep"
  }
}
