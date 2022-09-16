# HTTP Target

## Authentication

Where basicauth is used, it may be configured using the `basic_auth_username` and `basic_auth_password` options. Where an authorisation header is used, it may be set via the `headers` option.

we recommend using environment variables for sensitive values - which can be done via HCL's native `env.MY_ENV_VAR` format (as seen below).

TLS may be configured by providing the `key_file`, `cert_file` and `ca_file` options with paths to the relevant TLS files.

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for HTTP target (only required options)

target {
  use "http" {
    # URL endpoint
    url = "https://acme.com/x"
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.

Here is an example of every configuration option:

```hcl
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

```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.