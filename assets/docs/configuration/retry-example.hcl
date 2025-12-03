retry {
  transient {
    # Initial delay (before first retry) for transient errors (default: 1000)
    delay_ms = 5000 

    # Maximum number of retries for transient errors (default: 5)
    max_attempts = 10 

    # Whether to send the data to invalid after max retries (default: false)
    invalid_after_max = true
  }
  setup {
    # Initial delay (before first retry) for setup errors (default: 20000)
    delay_ms = 30000

    # Maximum number of retries for setup errors (default: 5)
    max_attempts = 3
  }
  throttle {
    # Initial delay (before first retry) for throttle errors (default: 20000)
    delay_ms = 30000

    # Maximum number of retries for throttle errors (default: 5)
    max_attempts = 3
  }
}
