# spEnrichedFilterContext Configuration

`spEnrichedFilterContext`: Filters messages based on a regex match against a field in a context.

This transformation is for use on fileds from contexts.

Note that if the same context is present in the data more than once, one instance of a match is enough for the regex condition to be considered a match - and the message to be kept.

The full parsed context name must be provided, in camel case, in the format returned by the Snowplow analytics SDK: `contexts_{vendor}_{name}_{major version}` - for example `contexts_nl_basjes_yauaa_context_1`.

The path to the field to be matched must then be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

The below example filters out events which contain `test-data` in the `test1` field of the `contexts_nl_basjes_yauaa_context_1` context. Note that the `contexts_nl_basjes_yauaa_context_1` context is attached more than once, if _any_ of the values at `test1` don't match `test-data`, the event will be kept.

// TODO: Make a new example, this one's too confusing
// Also don't use contexts_nl_basjes_yauaa_context_1 - it's also confusing to do so.

// TODO: unit test and template

Minimal configuration:

```hcl
transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_nl_basjes_yauaa_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "test1"

    # Regex pattern to match against. Matches will be kept
    regex = "^test-data.*$"
  }
}
```

Every configuration option:

```hcl
transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_nl_basjes_yauaa_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "test1"

    # Regex pattern to match against. Matches will be kept
    regex = "^test-data.*$"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns (eg negaitve lookahead) are less performant
    regex_timeout = 10
  }
}
```