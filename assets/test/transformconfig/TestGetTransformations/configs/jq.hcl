transform {
  use "jq" {
    jq_command = <<JQEOT
{
    my_app_id: .app_id,
}
JQEOT

    snowplow_mode = true
  }
}
