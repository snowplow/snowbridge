# Stdout has no required configuration options as a failure target.
# Since it is the default failure target, the failure_target block can be omitted.

failure_target {
  use "stdout" {}
}
