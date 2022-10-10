transform {
  use "spEnrichedFilterUnstructEvent" {

    unstruct_event_name = "add_to_cart"

    custom_field_path = "a.b.c"

    regex = "test-data1"

    filter_action = "keep"
  }
}

