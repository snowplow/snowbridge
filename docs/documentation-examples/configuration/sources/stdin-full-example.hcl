# Extended configuration for Stdin as a source (all options)
# Stdin only has one option, to set the concurrency

source {
  use "stdin" {
    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20
  }    
}