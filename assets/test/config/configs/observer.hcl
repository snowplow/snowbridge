# stats receiver extended configuration

stats_receiver {
  use "statsd" {
    address = "test.localhost"
    prefix  = "snowplow.test"
    tags    = "{\"testKey\": \"testValue\"}"
  }
  timeout_sec = 2
  buffer_sec  = 20
}
