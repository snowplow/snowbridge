# Failure model

## Failure targets

When Stream Replicator hits an unrecoverable error - for example [oversized](#oversized-data) or [invalid](#invalid-data) data - it will emit a [failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events#what-is-a-failed-event) to the configured failure target. A failure target is the same as a target, the only difference is that the configured destination will receive failed events.

You can find more detail on setting up a failure target, in the [configuration section](../configuration/failure-targets/)

## Failure cases

There are several different failures that Stream Replicator may hit:

### Target failure

This is where a request to the destination technology fails or is rejected - for example a http 400 response is received. When Stream Replicator hits this failure, it will retry 5 times. If all 5 attempts fail, it will be reported as a 'MsgFailed' for monitoring purposes, and will proceed without acking the failed Messages. As long as the source's acking model allows for it, these will be re-processed through Stream Replicator again.

Note that this means failures on the receiving end (eg. if an endpoint is unavailable), mean Stream Replicator will continue to attempt to process the data until the issue is fixed.


### Oversized data

Targets have limits to the size of a single message. Where the destination technology has a hard limit, targets are hardcoded to that limit. Otherwise, this is a configurable option in the target configuration. When a message's data is above this limit, stream-replicator will produce a [size violation failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#size-violation), and emit it to the failure target.

Writes of oversized messages to the failure target will be recorded with 'OversizedMsg' statistics in monitoring. Any failure to write to the failure target will cause a [fatal failure](#fatal-failure).

### Invalid data

In the unlikely event that Stream Replicator encounters data which is invalid for the target destination (for example empty data is invalid for pubsub), it will create a [generic error failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#generic-error),  emit it to the failure target, and ack the original message.

Transformation failures are also treated as invalid, as described below.

Writes of invalid messages to the failure target will be recorded with 'InvalidMsg' statistics in monitoring. Any failure to write to the failure target will cause a [fatal failure](#fatal-failure).

### Transformation failure

Where a transformation hits an exception, Stream Replicator will consider it invalid, assuming that the configured transformation cannot process the data. It will create a [generic error failed event](https://docs.snowplow.io/docs/managing-data-quality/failed-events/understanding-failed-events/#generic-error), emit it to the failure target, and ack the original message.

As long as the built-in transformations are configured correctly, this should be unlikely. For scripting transformations, Stream Replicator assumes that an exception means the data cannot be processed - make sure to construct and test your scripts accordingly.

Writes of invalid messages to the failure target will be recorded with 'InvalidMsg' statistics in monitoring. Any failure to write to the failure target will cause a [fatal failure](#fatal-failure).

### Fatal failure

Stream Replicator is built to be averse to crashes, but there are two scenarios where it would be expected to crash.

Firstly, if it hits an error in retrieving data from the source stream, it will log an error and crash. If this occurs it is normally a case of misconfiguration of the source. If that is not the case, it will be safe to redeploy the app - it will attempt to begin from the first unacked message. This may cause duplicates.

Secondly, as described above, where there are failures it will attempt to reprocess the data if it can, and where failures aren't recoverable it will attempt to handle that via a failure target. Normally, even reaching this point is rare.

In the very unlikely event that Stream Replicator reaches this point and cannot write to a failure target, the app will crash. Should this happen, and the app is re-deployed, it will begin processing data from the last acked message. Note that the likely impact of this is duplicated sends to the target, but not data loss. 

Of course, if you experience crashes or other issues that are not explained by the above, please log an issue detailing the bahaviour.