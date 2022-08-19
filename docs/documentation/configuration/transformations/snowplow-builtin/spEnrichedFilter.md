# spEnrichedFilter Configuration

`spEnrichedFilter`: Filters messages based on a regex match against an atomic field.

This transformation is for use on base-level atomic fields, rather than fields from contexts, or custom events - which can be achieved with `spEnrichedFilterContext` and `spEnrichedFilterUnstructEvent`.

This example filters out all data whose `platform` value does not match either `web` or `mobile`.

```hcl
transform {
  use "spEnrichedFilter" {
    atomic_field = "platform"
    regex = "web|mobile"
    regex_timeout = 10
  }
}
```

// TODO: Minimal and full config examples