transform {
  use "jq" {
    jq_command = <<JQEOT
{
    agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash,
}
JQEOT

    snowplow_mode = true
  }
}
