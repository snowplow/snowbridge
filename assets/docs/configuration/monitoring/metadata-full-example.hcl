monitoring {
 metadata_reporter {
   # An actual HTTP endpoint where metadata events would be sent
   endpoint = "https://webhook.metadata.com"

   # Set of arbitrary key-value pairs attached to the payload
   tags = {
     pipeline = "production"
   }
 }
}