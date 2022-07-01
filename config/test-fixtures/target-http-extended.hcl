# http target extended configuration

target {
  use "http" {
    url                        = "testUrl"
    byte_limit                 = 1000000
    request_timeout_in_seconds = 2
    content_type               = "test/test"
    headers                    = "{\"Accept-Language\":\"en-US\"}"
    basic_auth_username        = "testUsername"
    basic_auth_password        = "testPass"
    tls_cert                   = "dGVzdC5jZXJ0"
    tls_key                    = "dGVzdC5rZXkK"
    tls_ca                     = "dGVzdC5jYQ=="
    skip_verify_tls            = true
  }
}
