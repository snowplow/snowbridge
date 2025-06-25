transform {
  use "jq" {
    jq_command = <<JQEOT
{
    agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("sha1"; ""),
}
JQEOT

    snowplow_mode = true
  }
}
