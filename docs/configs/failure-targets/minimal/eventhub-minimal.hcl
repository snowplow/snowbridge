# Simple configuration for Eventhub as a failure target (only required options)

failure_target {
  use "eventhub" {
    # Namespace housing Eventhub
    namespace = "testNamespace"

    # Name of Eventhub
    name      = "testName"
  }
}
