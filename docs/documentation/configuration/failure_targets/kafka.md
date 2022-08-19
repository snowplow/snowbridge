# Configure a Kafka Failure Target

Failure targets are only used when stream replicator hits an unrecoverable failure. In such cases, errors are sent to the configured failure target, for debugging.

Apart from the fact that the app only sends information about unrecoverable failures to them, failure targets are the same as targets in all other respects.

## Authentication

Where SASL is used, it may be enabled via the `enable_sasl`, `sasl_username`, and `sasl_password` and `sasl_algorithm` options.

It is recommended to use environment variables for sensitive values - which can be done via HCL's native `env.MY_ENV_VAR` format (as seen below).

TLS may be configured by providing the `key_file`, `cert_file` and `ca_file` options with paths to the relevant TLS files.


## Configuration options

Here is an example of the minimum required configuration:

// TODO: add example configs and tests, and template for all of this.

```hcl

```

Here is an example of every configuration option:

TODO: use embed/template of configs/target/full/kafka-full.hcl

```hcl

```