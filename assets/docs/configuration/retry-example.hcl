retry {
  transient {
    delay_sec = 5 
    max_attempts = 10 
  }
  setup {
    delay_sec = 30
  }
}
