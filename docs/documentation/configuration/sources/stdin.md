# Stdin Source

Stdin source is the default, and has one optional configuration to set the concurrency. 

Stdin source simply treats stdin as the input.

## Configuration 

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for Stdin as a source
# Stdin has no required configuration options.
# Since it is the default source, the source block can also be omitted.

source {
  use "stdin" {}
}
```

Here is an example of every configuration option:

```hcl
# Extended configuration for Stdin as a source (all options)
# Stdin only has one option, to set the concurrency

source {
  use "stdin" {
    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 20
  }    
}
```