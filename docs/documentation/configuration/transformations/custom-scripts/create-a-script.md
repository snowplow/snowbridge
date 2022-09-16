# Guide to creating a custom script transformation

Custom tranformation scripts may be defined in Javascript or Lua, and provided to Stream Replicator.

## The scripting interface

The script - whether Lua or Javascript - must define a main function with a single argument. Stream Replicator will pass the engineProtocol data structure as the argument:


```go
type engineProtocol struct {
	FilterOut    bool
	PartitionKey string
	Data         interface{}
}
```

This data structure will serve as both the input and the output of the script - for Javascript it will be an object, and for Lua a table.

Scripts must define a main function with a single input argument:

```js
function main(input) {
    return input
}
```

```lua
function main(input)
  return input
end
```

## Accessing data

Scripts can access the message Data at `input.Data`, and can return modified data by returning it in the `Data` field of the output. Likewise for the partition key to be used for the destination - `input.PartitionKey` and the `PartitionKey` field of the output.

By default, the Data field will be a string. For Snowplow enriched TSV data, the `snowplow_mode` option transforms the data to an object for Javascript, or a table for Lua - field names will be those output by the Snowplow Analytics SDK.

The output of the script must be an object (Javascript) or a table (Lua) which maps to engineProtocol.

## Transforming Data

To modify the messages data, return an object which conforms to EngineProtocol, with the `Data` field set to the modified data. The `Data` field may be returned as either a string, or an object (Javascript) / table (Lua).

```js
function main(input) {
    // input is a string, so we parse it
    var jsonObj = JSON.parse(input.Data);
    
    // set the name field
    jsonObj.name = "Bruce Wayne"
    return {
        // Pass it back to Stream Replicator via the Data field
        Data: jsonObj
    };
}
```

```lua
function main(input)
    -- input is a string, so we parse it
    local json = require("json")
    local jsonObj, _ = json.decode(input.Data)

    -- set the name field
    jsonObj.name = "Bruce Wayne"

    -- Pass it back to Stream Replicator via the Data field
    return { Data = jsonObj }
end
```

## Filtering

If the `FilterOut` field of the output is returned as `true`, the message will be acked immediately and won't be sent to the target. This will be the behaviour regardelss of what is returned to the other fields in the protocol.

```js
function main(input) {
    return { FilterOut: true }
}
```

```lua
function main(input)
	return { FilterOut = true }
end
```

## Setting the Partition Key

To set the Partition Key in the message, you can simply set the input's PartitionKey field, and return it:

```js
function main(input) {
    input.PartitionKey = "myPk"
    return input
}
```

```lua
function main(input)
	input.PartitionKey = "myPk"
	return input
end
```

Or, if modifying the data as well, return the modified data and PartitionKey field:

```js
function main(input) {
    // input is a string, so we parse it
    var jsonObj = JSON.parse(input.Data);
    
    // set the name field
    jsonObj.name = "Bruce Wayne"
    return {
        // Pass it back to Stream Replicator via the Data field
        Data: jsonObj,
        PartitionKey: "myPk"
    };
}
```

```lua
function main(input)
    -- input is a string, so we parse it
    local json = require("json")
    local jsonObj, _ = json.decode(input.Data)

    -- set the name field
    jsonObj.name = "Bruce Wayne"

    -- Pass it back to Stream Replicator via the Data field
    return { Data = jsonObj, ParititionKey = "myPk" }
end
```

## Configuration

Once your script is ready, you can configure it in the app by following the [Javascript](./javascript-configuration.md) or [Lua](./lua-configuration.md) configuration pages.

You can also find some complete example use cases in [the examples section](./examples/).