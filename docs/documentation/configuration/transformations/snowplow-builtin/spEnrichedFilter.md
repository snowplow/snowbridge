# spEnrichedFilter Configuration

`spEnrichedFilter`: Filters messages based on a regex match against an atomic field.

This transformation is for use on base-level atomic fields, rather than fields from contexts, or custom events - which can be achieved with `spEnrichedFilterContext` and `spEnrichedFilterUnstructEvent`.

This example filters out all data whose `platform` value does not match either `web` or `mobile`.

Minimal configuration:

```hcl
transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "platform"

    # Regex pattern to match against. Matches will be kept
    regex = "web|mobile"
  }
}
```

Every configuration option:

```hcl
transform {
  use "spEnrichedFilter" {

    # Field to base the filter on - must be a base-level atomic field
    atomic_field = "platform"

    # Regex pattern to match against. Matches will be kept
    regex = "web|mobile"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns are less performant
    regex_timeout = 10
  }
}
```