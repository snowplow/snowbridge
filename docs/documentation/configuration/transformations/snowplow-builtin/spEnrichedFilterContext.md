# spEnrichedFilterContext Configuration

`spEnrichedFilterContext`: Filters messages based on a regex match against a field in a context.

This transformation is for use on fileds from contexts.

Note that if the same context is present in the data more than once, one instance of a match is enough for the regex condition to be considered a match - and the message to be kept.

The full parsed context name must be provided, in camel case, in the format returned by the Snowplow analytics SDK: `contexts_{vendor}_{name}_{major version}` - for example `contexts_nl_basjes_yauaa_context_1`.

The path to the field to be matched must then be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

The below example keeps messages which contain `prod` in the `environment` field of the `contexts_com_acme_env_context_1` context. Note that the `contexts_com_acme_env_context_1` context is attached more than once, if _any_ of the values at `dev` don't match `environment`, the message will be kept.

Minimal configuration:

```hcl
transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_com_acme_env_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "environment"

    # Regex pattern to match against. Matches will be kept
    regex = "^prod$"
  }
}
```

Every configuration option:

```hcl
transform {
  use "spEnrichedFilterContext" {
    # Full name of the context to match against
    context_full_name = "contexts_com_acme_env_context_1"

    # Path to the field to filter on, within the context
    custom_field_path = "environment"

    # Regex pattern to match against. Matches will be kept
    regex = "^prod$"

    # Regex timeout - if the regex takes longer than this timeout (in seconds), the transformation fails
    # This exists as certain regex patterns are less performant
    regex_timeout = 10
  }
}
```