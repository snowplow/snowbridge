# Custom Javascript Script Configuration

The Custom Javascript Script transformation uses the [goja](https://pkg.go.dev/github.com/dop251/goja) embedded Javascript engine to run scripts upon the data.

You can find a guide to writing the scripts themselves - and how the script interfaces with the application - [in the create a script page](./create-a-script.md).

If a script errors or times out, a [transformation failre](../../../concepts/failure-model.md) occurs.

Scripts are provided to the configuration as base-64 encoded strings.

Here is an example of a minimal configuration for the custom Javascript script transformation:

```hcl
transform {
  use "js" {
    # The script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required)
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="
  }
}
```

Here is an example of every configuration option:

```hcl
# transform configuration
transform {
  use "js" {

    # The script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required) 
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec         = 20

    # optional, disables JS source maps, which allow access to the filesystm. It is recommended to set to true where possible.
    disable_source_maps = true

    # optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode       = false
  }
}
```
