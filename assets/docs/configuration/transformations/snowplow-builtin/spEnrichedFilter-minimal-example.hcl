transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "platform"

    # Regex pattern to match against. Matches will be kept
    regex = "web|mobile"

    # Specifies the behaviour of the filter on a match:
    # "keep" continues to process the message to the target when the regex is matched, 
    # "drop" acks the message immediately and does not send it to the target.
    filter_action = "keep"
  }
}
