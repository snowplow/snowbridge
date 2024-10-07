retry {
  transient {
    # Initial delay (before first retry) for transient errors
    delay_ms = 5000 

    # Maximum number of retries for transient errors
    max_attempts = 10 
  }
  setup {
    # Initial delay (before first retry) for setup errors
    delay_ms = 30000
  }
}
