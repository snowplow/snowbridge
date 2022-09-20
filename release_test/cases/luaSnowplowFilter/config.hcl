transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICAgIGlmIGlucHV0WyJEYXRhIl1bImFwcF9pZCJdIH49ICJ0ZXN0LWRhdGExIiB0aGVuCiAgICAgICAgcmV0dXJuIHtGaWx0ZXJPdXQgPSB0cnVlfQogICAgZWxzZQogICAgICAgIHJldHVybiBpbnB1dAogICAgZW5kCmVuZA=="
    
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}