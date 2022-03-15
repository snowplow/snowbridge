# Simple configuration for HTTP as a failure target (only required options)

failure_target {
  use "http" {
    # URL endpoint
    url = "https://acme.com/x"
  }
}
