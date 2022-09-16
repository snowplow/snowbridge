# Failure model

## Failure targets

When Stream Replicator hits an unrecoverable error - for example oversized or invalid data - it will emit information a [failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events#what-is-a-failed-event) to the configured failure target. A failure target is the same as a target, the only difference is that the configured destination will receive failed events.

You can find more detail on setting up a failure target, in the [getting started section](../configuration/failure-targets/)

## Failure cases

There are several different failures that Stream Replicator may hit:

### Target failureß

This is where a request to the destination technology fails or is rejected - for example a http 400 response is received. When Stream Replicator hits this failure, it will retry 5 times. If all 5 attempts fail, it will be reported as a 'MsgFailed' for monitoring purposes, and will proceed without acking those Messages. As long as the source's acking model allows for it, these will be re-processed through Stream Replicator again.

Note that this means failures on the receiving end (eg. if an endpoint is unavailable), then Stream Replicator will continue to attempt to process the data until the issue is fixed.


### Oversised data

Targets have limits to the size of a single message or request. Where the destination technology has a hard limit, targets are hardcoded to that limit. Otherwise, this is a configurable option in the target configuration. When a message's data is above this limit, stream-replicator will produce a [size violation failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#size-violation), and emit it to the failure target.

Writes of oversized messages to the failure target will be recorded with 'OversizedMsg' statistics in monitoring. Any failure to write to the failure target will cause Stream Replicator to crash.

### Invalid data

In the unlikely event that Stream Replicator encounters data which is invalid for the target destination (for example empty data is invalid for pubsub), it will create a [generic error failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#generic-error),  emit it to the failure target, and ack the original message.

Transformation failures are also treated as invalid, as described below.

Writes of invalid messages to the failure target will be recorded with 'InvalidMsg' statistics in monitoring. Any failure to write to the failure target will cause Stream Replicator to crash.

### Transformation failure

Where a transformation hits an exception, Stream Replicator will consider it invalid, assuming that the configured transformation cannot process the data. It will create a [generic error failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#generic-error), emit it to the failure target, and ack the original message.

As long as the built-in transformations are configured correctly, this should be unlikely. For scripting transformations, Stream Replicator assumes that an exception means the data cannot be processed - make sure to construct and test your scripts accordingly.

Writes of invalid messages to the failure target will be recorded with 'InvalidMsg' statistics in monitoring. Any failure to write to the failure target will cause Stream Replicator to crash.