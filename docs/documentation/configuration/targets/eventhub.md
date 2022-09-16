# EventHub Target

## Authentication

Authentication for the EventHub target is done by configuring any valid combination of the environment variables [listed in the Azure EventHubs Client documentation](https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment).

## Configuration options

Here is an example of the minimum required configuration:

```hcl
# Minimal configuration for Eventhub as a target (only required options)

target {
  use "eventhub" {
    # Namespace housing Eventhub
    namespace = "testNamespace"

    # Name of Eventhub
    name      = "testName"
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.

Here is an example of every configuration option:

```hcl
# Extended configuration for Eventhub as a target (all options)

target {
  use "eventhub" {
    # Namespace housing Eventhub
    namespace                  = "testNamespace"

    # Name of Eventhub
    name                       = "testName"

    # Number of retries handled automatically by the EventHubs library.
    # All retries should be completed before context timeout (default: 1).
    max_auto_retries           = 2

    # Default presumes paid tier byte limit is 1MB (default: 1048576)
    message_byte_limit         = 1048576

    # Chunk byte limit (default: 1048576)
    chunk_byte_limit           = 1048576

    # Chunk message limit (default: 500)
    chunk_message_limit        = 500

    # The time (seconds) before context timeout (default: 20)
    context_timeout_in_seconds = 20

    # Default batch size of 1MB is the limit for Eventhub's high tier (default: 1048576)
    batch_byte_limit           = 1048576

    # Sets the eventHub message partition key, which is used by the EventHub client's batching strategy
    set_eh_partition_key = true
  }
}
```

If you want to use this as a [failure target](../../concepts/failure-model.md#failure-targets), then use failure_target instead of target.