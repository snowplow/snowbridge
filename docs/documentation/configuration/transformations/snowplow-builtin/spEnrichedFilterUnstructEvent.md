# spEnrichedFilterUnstructEvent Configuration

`spEnrichedFilterUnstructEvent`: Filters messages based on a regex match against a field in a custom event.

This transformation is for use on fields from custom events.

The event name must be provided as it appears in the `event_name` field of the event (eg. `add_to_cart`). 

Optionally, a regex can be provided to match against the stringified version of the event (eg. `1-*-*`)

The path to the field to match against must be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

This example keeps all events whose `add_to_cart` event data at the `sku` field matches `test-data`:

```hcl
transform {
  use "spEnrichedFilterUnstructEvent" {
    unstruct_event_name = "unstruct_event_add_to_cart"
    custom_field_path = "sku"
    regex = "test-data"
    regex_timeout = 10
  }
}
```

// TODO: Minimal and full config examples