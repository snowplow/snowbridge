# transform configuration
transform {
  use "js" {

    # The script encoded in Base 64. The script must define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required) 
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec         = 20

    # optional, disables JS source maps, which allow access to the filesystm. 
    disable_source_maps = true

    # optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode       = false
  }
}