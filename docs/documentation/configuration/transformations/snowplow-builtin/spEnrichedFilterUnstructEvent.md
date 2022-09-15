# spEnrichedFilterUnstructEvent Configuration

`spEnrichedFilterUnstructEvent`: Filters messages based on a regex match against a field in a custom event.

This transformation is for use on fields from custom events.

The event name must be provided as it appears in the `event_name` field of the event (eg. `add_to_cart`). 

Optionally, a regex can be provided to match against the stringified version of the event (eg. `1-*-*`)

The path to the field to match against must be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

This example keeps all events whose `add_to_cart` event data at the `sku` field matches `test-data`:

Minimal configuration:

```hcl
transform {
  use "spEnrichedFilterUnstructEvent" {
    # Event name for custom event - this will match against the `event_name` field
    unstruct_event_name = "add_to_cart"

    # Path to the field to filter on, within the custom event
    custom_field_path = "sku"

    # Regex to match. Only matches against this regex are kept
    regex = "test-data"
  }
}
```

Every configuration option:

```hcl
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
```


