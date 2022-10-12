transform {
  use "spEnrichedFilterUnstructEvent" {

    unstruct_event_name = "link_click"

    custom_field_path = "targetUrl"

    regex = "^https"
    
    filter_action = "keep"
  }
}

disable_telemetry = true