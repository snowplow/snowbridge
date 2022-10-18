transform {
  use "spEnrichedFilterContext" {

    context_full_name = "com_snowplowanalytics_snowplow_web_page_1"

    custom_field_path = "a.b.c"

    regex = "test-data1"

    filter_action = "keep"
  }
}


