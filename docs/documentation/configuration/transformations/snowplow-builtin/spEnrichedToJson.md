# SpEnrichedToJson configuration

`spEnrichedToJson`: Transforms a message's data from Snowplow Enriched tsv string format to a JSON object. The input data must be a valid Snowplow enriched TSV.

spEnrichedToJson requires no further configuration.

Example:

```hcl
transform {
  use "spEnrichedToJson" {
  }
}
```