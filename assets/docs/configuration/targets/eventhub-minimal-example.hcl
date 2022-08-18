# Minimal configuration for Eventhub as a target (only required options)

target {
  use "eventhub" {
    # Namespace housing Eventhub
    namespace = "testNamespace"

    # Name of Eventhub
    name      = "testName"
  }
}
