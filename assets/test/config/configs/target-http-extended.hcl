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
    cert_file                  = "myLocalhost.crt"
    key_file                   = "MyLocalhost.key"
    ca_file                    = "myRootCA.crt"
    skip_verify_tls            = true
  }
}
