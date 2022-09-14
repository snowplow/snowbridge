# Stdout Failure Target


Failure targets are only used when stream replicator hits an unrecoverable failure. In such cases, errors are sent to the configured failure target, for debugging.

Apart from the fact that the app only sends information about unrecoverable failures to them, failure targets are the same as targets in all other respects.

Stdout failure target doesn't have any configurable options - when configured it simply outputs the messages to stdout.

## Configuration 

Here is an example of the configuration:

```hcl
# Extended configuration for Stdout as a target (all options)

failure_target {
  use "stdout" {}
}
```
