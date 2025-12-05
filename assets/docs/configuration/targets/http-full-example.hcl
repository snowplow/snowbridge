# Extended configuration for HTTP target (all options)

target {
  use "http" {
    # URL endpoint
    url                        = "https://acme.com/x"

    # Maximum number of events that can go into one batched request (default: 20)
    request_max_messages       = 100

    # Byte limit for requests (default: 1048576)
    request_byte_limit         = 1000000

    # Byte limit for individual messages (default: 1048576)
    message_byte_limit         = 1000000

    # Request timeout in milliseconds (default: 5000)
    # Takes precedence over `request_timeout_in_seconds` (when both defined)  
    request_timeout_in_millis  = 2000

    # Request timeout in seconds (default: 5)
    # Deprecated, use `request_timeout_in_millis` instead
    request_timeout_in_seconds = 2

    # Content type for POST request (default: "application/json")
    content_type               = "text/html"

    # Optional headers to add to the request.
    # It is provided as a JSON string of key-value pairs (default: "").
    headers                    = "{\"Accept-Language\":\"en-US\"}"

    # Optional basicauth username
    basic_auth_username        = "myUsername"

    # Optional basicauth password
    # Even though you could just reference the password directly as a string,
    # you could also reference an environment variable.
    basic_auth_password        = env.MY_AUTH_PASSWORD

    # Whether to enable TLS
    enable_tls = true

    # The optional certificate file for client authentication
    cert_file                  = "myLocalhost.crt"

    # The optional key file for client authentication
    key_file                   = "myLocalhost.key"

    # The optional certificate authority file for TLS client authentication
    ca_file                    = "myRootCA.crt"

    # Whether to skip verifying ssl certificates chain (default: false)
    # If tls_cert and tls_key are not provided, this setting is not applied.
    skip_verify_tls            = true

    # Whether to enable setting headers dynamically
    dynamic_headers            = true

    # Optional. One of client credentials required when authorizing using OAuth2.
    oauth2_client_id           = env.CLIENT_ID

    # Optional. One of client credentials required when authorizing using OAuth2.
    oauth2_client_secret       = env.CLIENT_SECRET

    # Optional. Required when using OAuth2. Long-lived token used to generate new short-lived access token when previous one experies.
    oauth2_refresh_token       = env.REFRESH_TOKEN

    # Optional. Required when using OAuth2. URL to authorization server providing access token. E.g. for Goggle API "https://oauth2.googleapis.com/token"  
    oauth2_token_url           = "https://my.auth.server/token"

    # Optional path to the file containing template which is used to build HTTP request based on a batch of input data
    template_file              = "myTemplate.file"

    # Optional. When enabled, precalculated rejection timestamp is attached as HTTP header. This option is part of a feature of the Snowplow Event Forwarding product.
    include_timing_headers     = true

    # Optional. Used in combination with `include_timing_headers` to precalculate rejection timestamp. 
    rejection_threshold_in_millis = 100

    # Optional HTTP response rules which are used to match HTTP response code/body and categorize it as either invalid data or target setup error.
    # Rules are evaluated in order as declared. First matching rule determines the error type.
    response_rules {
      # Invalid rule for purchase field validation error
      rule {
          type = "invalid"
          http_codes = [400]
          body =  "Invalid value for 'purchase' field"
        }
      # Throttle rule for 429 throttle response
      rule {
          type = "throttle"
          http_codes = [429]
        }
      # Setup rule for authentication errors
      rule {
          type = "setup"
          http_codes =  [401, 403]
        }
      # Example rule to treat client timeouts as throttling
      rule {
        type = "throttle"
        http_codes = [0]
        body = "context deadline exceeded"
      }
    }
    
  # Optional. When enabled, safe strings are used for metadata error reporting. If disabled, response bodies are used. (default: true)
  # Used where API responses may contain sensitive data, which shouldn't pass through the metadata reporter.
  metadata_safe_mode = true
  }
}
