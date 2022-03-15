# Extended configuration for Stdin as a source (all options)

source {
  use "stdin" {
    # Number of events to process concurrently (default: 50)
    concurrent_writes = 20
  }
}
