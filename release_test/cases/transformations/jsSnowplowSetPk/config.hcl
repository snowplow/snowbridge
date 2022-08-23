transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgLy8gaW5wdXQgaXMgYW4gb2JqZWN0CiAgICB2YXIgc3BEYXRhID0gaW5wdXQuRGF0YTsKICAgIHJldHVybiB7CiAgICAgICAgRGF0YTogc3BEYXRhLAogICAgICAgIFBhcnRpdGlvbktleTogc3BEYXRhWyJldmVudF9pZCJdCiAgICB9Owp9"
    
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}

disable_telemetry = true