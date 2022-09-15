# Transformations and filters

Transformations allow you to modify data on the fly before they're sent to the destination. There are a set of built-in transformations, specifically for use with [Snowplow](https://snowplow.io/) data (for example transforming Snowplow enriched events to JSON), You can also configure a script to transform your data however you require - for example if you need to rename fields or change a field's format.

It's also possible to exclude messages (ie. not send them to the target) based on a condition, by configuring a special type of transformation called a filter. (Technically then, filters are transformations, but we sometimes refer to them as a separate concept for clarity). Again there are built-in filters to apply to Snowplow data, or you can provide a script to do filter the data.

Transformations operate on a per-message basis, are chained together in the order configured, and the same type of transformation may be configured more than once. It is advisable to place filters first for performance reasons. When transformations are chained together, the output of the first is the input of the second, however transformations may not depend on each other in any other way. 

For example, if you have a built-in filter with condition A, and a filter with condition B, I may arrange them one after another, so that the data must satisfy A AND B. But you can't arrange them to satisfy A OR B - because the outcome of each must be determined on their own.

The latter use case, and further nuanced use cases can, however, be achieved using scripting transformation.

# Custom Scripting transformations

Custom scripting transformations allow the user to provide a script to transform the data, set the destination's partition key, or filter the data according to their own custom logic. Scripts may be provided in Lua or Javascript. For each script provided, a runtime engine is used to run the script against the data. Scipts interface with the rest of the app via the EngineProtocol interface, which provides a means to pass data into the scripting layer, and return data from the scripting layer back to the app.

You can find more detail on setting up custom scripts [in the getting started section](../getting_started/transformations/overview.md)