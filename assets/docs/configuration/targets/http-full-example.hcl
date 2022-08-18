# Extended configuration for HTTP target (all options)

target {
  use "http" {
    # URL endpoint
    url                        = "https://acme.com/x"

    # Byte limit for requests (default: 1048576)
    byte_limit                 = 1048576

    # Request timeout in seconds (default: 5)
    request_timeout_in_seconds = 5

    # Content type for POST request (default: "application/json")
    content_type               = "application/json"

    # Optional headers to add to the request.
    # It is provided as a JSON string of key-value pairs (default: "").
    headers                    = "{\"Accept-Language\":\"en-US\"}"

    # Optional basicauth username
    basic_auth_username        = "myUsername"

    # Optional basicauth password
    # Even though you could just reference the password directly as a string,
    # you could also reference an environment variable.
    basic_auth_password        = env.MY_AUTH_PASSWORD

    # The optional certificate file for client authentication
    cert_file                  = "myLocalhost.crt"

    # The optional key file for client authentication
    key_file                   = "MyLocalhost.key"

    # The optional certificate authority file for TLS client authentication
    ca_file                    = "myRootCA.crt"

    # Whether to skip verifying ssl certificates chain (default: false)
    # If tls_cert and tls_key are not provided, this setting is not applied.
    skip_verify_tls            = true
  }
}

