transform {
  use "spGtmssPreview" {
    # Message expiry time in seconds (comparing current time to the message's collector timestamp). If message is expired, it's sent to failure target. 
    expiry_seconds = 600
  }
}
