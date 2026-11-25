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

   # Whether to attach a sample to each distinct invalid error in the metadata event,
   # for at most 10 distinct errors per reporting period. The sample is the failed event
   # produced for the first occurrence; with the event_forwarding failure format it holds
   # the original input, the latest transformed state and the full error (default: false)
   include_error_samples = true
 }
}