monitoring {
 webhook {
   # An actual HTTP endpoint where monitoring events would be sent
   endpoint = "https://webhook.acme.com"

   # Set of arbitrary key-value pairs attached to the payload
   tags = {
     pipeline = "production"
   }

   # How often to send the heartbeat event
   heartbeat_interval_seconds = 3600

   # Request timeout, in milliseconds, for sending a monitoring event (default: 5000)
   request_timeout_in_millis = 5000
 }
}