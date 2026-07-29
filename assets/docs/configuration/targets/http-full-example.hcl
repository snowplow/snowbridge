# Extended configuration for HTTP target (all options)

target {
  use "http" {
    batching {
      # Maximum number of events that can go into one batched request (default: 50)
      max_batch_messages     = 20
      # Maximum byte limit for a single batched request (default: 1048576)
      max_batch_bytes        = 10000000
      # Maximum byte limit for individual message (default: 1048576)
      max_message_bytes      = 10000000
      # How many batches attempted concurrently (default: 5)
      max_concurrent_batches = 2
      # Milliseconds between flushes of messages (default: 200)
      flush_period_millis    = 100
    }
    # URL endpoint
    url                        = "https://acme.com/x"

    # Request timeout in milliseconds (default: 5000)
    request_timeout_in_millis  = 2000

    # HTTP/2 idle health-check (only applies when HTTP/2 is negotiated).
    # Send a PING after this many milliseconds with no frames received (default: 5000).
    # Set to 0 to disable the health-check entirely.
    http2_send_ping_timeout_in_millis = 5000
    # Close the connection if a PING goes unanswered for this many milliseconds (default: 3000).
    # A value of 0 does not disable it — Go falls back to a 15000ms wait.
    http2_ping_timeout_in_millis      = 3000

    # Content type for POST request (default: "application/json")
    content_type               = "text/html"

    # Optional headers to add to the request as key-value pairs.
    headers = {
      Accept-Language = "en-US"
    }

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

    # Whether to enable setting headers dynamically - useful for enabling GTM SS debug mode
    # Events with dynamic headers will be sent individually, rather than batched.
    dynamic_headers            = true

    # Optional. OAuth2 client credentials flow. Mutually exclusive with oauth_jwt block.
    oauth_client {
      # Client ID for OAuth2.
      client_id     = env.CLIENT_ID

      # Client secret for OAuth2.
      client_secret = env.CLIENT_SECRET

      # Long-lived token used to obtain new short-lived access tokens.
      refresh_token = env.REFRESH_TOKEN

      # URL to the authorization server providing access tokens.
      token_url     = "https://my.auth.server/token"
    }

    # Optional. OAuth2 flow with JWT bearer. Mutually exclusive with oauth_client block.
    # Supports RSA (PKCS#1, PKCS#8) and EC private keys in PEM format.
    oauth_jwt {
      # PEM-encoded private key supplied as a string. Mutually exclusive with key_file.
      key      = "-----BEGIN RSA PRIVATE KEY-----\nmyBase64EncodedKey\n-----END RSA PRIVATE KEY-----"

      # Path to PEM-encoded private key file. Mutually exclusive with key.
      key_file = "myServiceAccount.pem"

      # Issuer claim (iss) — typically a service account email or client identifier.
      issuer   = "myservice@my-project.iam.gserviceaccount.com"

      # Optional. Subject claim (sub) — used for impersonation flows.
      subject  = "mysubject@example.com"

      # Optional. Audience claim (aud). Defaults to token_url if not set.
      audience = "https://my.auth.server/token"

      # Optional. Scopes to request in the token exchange.
      scopes   = ["https://www.googleapis.com/auth/dataportability"]

      # URL to the authorization server providing access tokens.
      token_url = "https://my.auth.server/token"
    }

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
      # Fatal rule to signal graceful shutdown when code is received
      rule {
          type = "fatal"
          http_codes =  [413]
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
