retry {
  transient {
    # Initial delay (before first retry) for transient errors (default: 1000)
    delay_ms = 5000 

    # Maximum number of retries for transient errors (default: 5)
    max_attempts = 10 
  }
  setup {
    # Initial delay (before first retry) for setup errors (default: 20000)
    delay_ms = 30000

    # Maximum number of retries for transient errors (default: 5)
    max_attempts = 3
  }
}
