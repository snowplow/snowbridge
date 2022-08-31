# Concepts

## Architecture Overview

Stream replicator's architecture is relatively simple - sources read from the input stream, and spawn goroutines for batches of data - within these goroutines, 0 or more transformations operate on the data (on a per-event basis, chained one after another). Targets are then called within the same goroutine, and are responsible for first checking that the data is under the configured size limit, and that it's valid data - then sending to the target. Oversized or invalid data is sent to a failure target in Snowplow bad row format.



// TODO: Feedback on & tidy up of arch diagram

![draft_architecture](assets/draft_sr_architecture.jpg)

## Concepts

### Sources

Sources are essentially plugins to deal with consuming the input stream, and spawning goroutines within which transformations and targets to operate. Concurrency is throttled at source, and is configurable.

// TODO: Note that this part is something that'll have to be manually updated when we get to changing the behaviour - is there a way to rely on something better than us remembering?
// - One option is to add a note to the issue - perhaps we can tag issues which require manual doc updates?

Currently, stream replicator does not manually batch data - if data is received from the source in batches, it forwards messages in those batches for processing. If the data is not batched on input (as is the case with the kinesis source), at present it will operate in single-event batches.

### Targets

Targets are plugins to deal with checks for validity and size restrictions, and sending data to the target. If data is provided in batches, and where the target client suits batching, the user can configure the 'chunk' size of batches sent to the target (as distinct from the size of batches received at input).

### Failure model

Failure targets are instances of targets specifically designed for unprocessable data - for example if a message is too large for the target, or is invalid. In this scenario the message will be wrapped in a Snowplow failed events wrapper and emitted to the configured failure target.

If a message is valid, however, a failure will not be sent to the failure target. Rather, we will not ack the message, and the event will be re-processed eventually. The specifics of how this mechanism will behave is left to the acking model and configuration of the source. Note that this design decision leaves scope for duplicate data to be sent in a failure scenario, for the benefit of avoiding data loss.

### Transformations and filters

Transformations are a process which can filter or transform the data in-flight. Filters are transformations which check for a filter condition - if the condition is satisfied, the filter will ack the message immediately and remove it from the queue for sending to the target. While we may sometimes refer to filters as a separate concept for more understandable documenation, within the codebase filters are conceptually a type of transformation.

Transformations operate on a per-event basis, and are chained together in the order in which they're configured. It is generally advisable to place filters first where possible, for the most efficient configuration.

The same type of transformation may be configured more than once - for example you may want to configure two filters to satisfy two different conditions.

Transformations do not have an awareness of each others' state - so a filter cannot depend on the outcome of another filter, for example. Each transformation is a self-contained piece of logic which must determine its own outcome. More complex transformation and filtering logic may be instrumented via the custom Lua and JS scripting transformations. See the scripting transformation interface section for more detail. 

// TODO: organisation of information - where's the best place to go detailed on scripting, and how should it be referred to here?

### Custom Scripting transformations

Custom scripting transformations allow the user to provide a script to transform the data, set the partition key, or filter the data according to their own custom logic. Scripts may be provided in Lua or Javascript. For each script provided, a runtime engine is used to run the script against the data. Scipts interface with the rest of the app via the EngineProtocol interface, which provides a means to pass data into the scripting layer, and return data from the scripting layer back to the app.

For more detail on using custom scripts, see // TODO: WHERE DOES THE DETAIL GO?


