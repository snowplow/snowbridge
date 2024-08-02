retry {
  transient {
    delay_ms = 5000 
    max_attempts = 10 
  }
  setup {
    delay_ms = 30000
  }
}
