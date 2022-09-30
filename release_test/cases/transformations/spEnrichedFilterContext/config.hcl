transform {
  use "spEnrichedFilterContext" {

    context_full_name = "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1"
    custom_field_path = "useragentMinor"
    regex = "^4$"
  }
}

disable_telemetry = true