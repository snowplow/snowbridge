# experimental: Configuration for HTTP as a source (only required options)
source {
  use "http" {
    # Specify where HTTP server should be bind to
    url = "localhost:8080"
  }
}
