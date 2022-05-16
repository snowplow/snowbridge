# Extended sentry configuration (all options)

sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://acme.com/1"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}
