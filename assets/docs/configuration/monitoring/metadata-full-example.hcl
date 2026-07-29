monitoring {
 metadata_reporter {
   # An actual HTTP endpoint where metadata events would be sent
   endpoint = "https://webhook.metadata.com"

   # Set of arbitrary key-value pairs attached to the payload
   tags = {
     pipeline = "production"
   }

   # Request timeout, in milliseconds, for sending a metadata event (default: 5000)
   request_timeout_in_millis = 5000
 }
}