# Custom Lua Script Configuration


The Custom Lua Script transformation uses the [gopher-lua](https://pkg.go.dev/github.com/yuin/gopher-lua) embedded Lua engine to run scripts upon the data.

You can find a guide to writing the scripts themselves - and how the script interfaces with the application - [in the create a script page](./create-a-script.md).

If a script errors or times out, a [transformation failre](../../../concepts/failure-model.md) occurs.

Scripts are provided to the configuration as base-64 encoded strings.

Here is an example of a minimal configuration for the custom Lua script transformation:

// TODO: Replace with tested config?

```hcl
# lua configuration
transform {
  use "lua" {

    # The script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required)   
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICByZXR1cm4gaW5wdXQKZW5k"

  }
}
```

Here is an example of every configuration option:

```hcl
# lua configuration
transform {
  use "lua" {

    # The script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required)   
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICByZXR1cm4gaW5wdXQKZW5k"

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec = 20

    # if true, libraries are not opened by default. Otherwise, the default [gopher-lua](https://github.com/yuin/gopher-lua/blob/658193537a640772633e656f4673334fe1644944/linit.go#L31-L42) libraries are loaded, in addition to [gopher-json](https://pkg.go.dev/layeh.com/gopher-json).
    sandbox     = true

    # optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode = false
  }
}
```
