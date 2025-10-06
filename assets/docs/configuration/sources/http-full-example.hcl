# experimental: Configuration for HTTP as a source
source {
  use "http" {
    # Specify where HTTP server should be bind to
    url = "localhost:8080"

    # Specify receiver endpoint path (default: /)
    path = "/receiver"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 15

    # Maximum number of events allowed in a single incoming request (default: 50)
    request_batch_limit = 15
  }
}
