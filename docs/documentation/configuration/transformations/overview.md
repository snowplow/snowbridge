# Transformations Configuration Overview

You can configure any number of transformations to run on the data one after another - transformations will run in the order provided. The same type of transformation may run more than once. All transformations operate on a single event basis.

If you're filtering the data, it's best to provide the filter first, for efficiency.

If you're working with Snowplow enriched events, you can configure scripting transformations, or any of the built-in transformations, which are specific to Snowplow data.

If you're working with any other type of data, you can create transformations via scripting transformations.

## Transformations and filters

Transformations modify messages in-flight. They might rename fields, perform computations, set partition keys, or modify data. For example if I wanted to change a `snake_case` field name to `camelCase`, I would use a transformation to do this.

Filters are a type of transformation, which prevent stream replicator for further processing data based on a condition. When data is filtered, stream replicator will ack the message without sending it to the target. For example if I only wanted to send page views to the destination, I would set up a filter with a condition where `event_name` must match the string `page_view`.

## Transformation Configuration

To configure transformations, supply one or more `transform {}` block. Choose the transformation using `use "{transformation_name}"`. 

Example:

The below first filters out any `event_name` which does not match the regex `^page_view$`, then runs a custom javascript script to change the app_id value to `"1"`

// TODO: instrument unit tests and template example

```hcl
transform {
  use "spEnrichedFilter" {
    # keep only page views
    atomic_field = "event_name"
    regex = "^page_view$"
  }
}

transform {
  use "js" {
    # changes app_id to "1"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gIjEiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}
```