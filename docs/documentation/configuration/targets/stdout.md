# Configure a Stdout Target

Stdout target doesn't have any configurable options - when configured it simply outputs the messages to stdout.
## Configuration 

Here is an example of the configuration:

TODO: use embed/template of configs/target/minimal/sqs-minimal.hcl

```hcl
# Simple configuration of SQS as a target (only required options)

target {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}
```
