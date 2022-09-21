transform {
  use "spEnrichedFilterUnstructEvent" {

    unstruct_event_name = "add_to_cart"

    custom_field_path = "sku"

    regex = "item41"
  }
}

disable_telemetry = true