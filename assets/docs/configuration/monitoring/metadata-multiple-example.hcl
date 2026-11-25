monitoring {
 # metadata_reporter can be repeated to send the same metadata to several endpoints,
 # each with its own settings. Reporters send concurrently.
 metadata_reporter {
   endpoint = "https://webhook.metadata.com"

   tags = {
     pipeline = "production"
   }
 }

 metadata_reporter {
   endpoint = "https://samples.metadata.com"

   include_error_samples = true
 }
}
