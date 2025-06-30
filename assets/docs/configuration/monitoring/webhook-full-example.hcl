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
 }
}