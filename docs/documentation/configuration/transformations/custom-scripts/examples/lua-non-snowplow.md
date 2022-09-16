# Lua Example - Non-Snowplow Data

For this example, the input data is a json string which looks like this: 

```json
{
  "name": "Bruce",
  "id": "b47m4n",
  "batmobileCount": 1
}
```

The script filters out any data with a `batmobileCount` less than 1, otherwise it updates the Data's `name` field to "Bruce Wayne", and sets the PartitionKey to the value of `id`:

```lua
function main(input)
	local json = require("json")
	local jsonObj, _ = json.decode(input.Data)
	if jsonObj.batmobileCount < 1 then 
		return {Data = "", FilterOut = true}
	end
	jsonObj.name = "Bruce Wayne"
	return { Data = jsonObj, PartitionKey = jsonObj.id }
end
```

The configuration for this script is:

```hcl 
transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KQoJbG9jYWwganNvbiA9IHJlcXVpcmUoImpzb24iKQoJbG9jYWwganNvbk9iaiwgXyA9IGpzb24uZGVjb2RlKHguRGF0YSkKCWlmIGpzb25PYmouYmF0bW9iaWxlQ291bnQgPCAxIHRoZW4gCgkJcmV0dXJuIHtEYXRhID0gIiIsIEZpbHRlck91dCA9IHRydWV9CgllbmQKCWpzb25PYmoubmFtZSA9ICJCcnVjZSBXYXluZSIKCXJldHVybiB7IERhdGEgPSBqc29uT2JqLCBQYXJ0aXRpb25LZXkgPSBqc29uT2JqLmlkIH0KICBlbmQ="

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}
```