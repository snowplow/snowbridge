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
    cert_file                  = "test.cert"
    key_file                   = "test.key"
    ca_file                    = "test.ca"
    skip_verify_tls            = true
  }
}
