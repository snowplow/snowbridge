# Stdout Target

Stdout target doesn't have any configurable options - when configured it simply outputs the messages to stdout.
## Configuration 

Here is an example of the configuration:

```hcl
# Extended configuration for Stdout as a target (all options)

target {
  use "stdout" {}
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target. 
