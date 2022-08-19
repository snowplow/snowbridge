# Custom Lua Script Configuration

Configuration options for Lua scripting are:

// TODO: Replace with tested config?

1. `source_b64`: required, the Lua script encoded in B64. The scriptmust define a `main` function which takes one argument and returns a table mapping to engineProtocol.
2. `timeout_sec`: optional, the timeout in seconds of the Lua script.
3. `sandbox`: optional, if true, libraries are not opened by default. Otherwise, the default [gopher-lua](https://github.com/yuin/gopher-lua/blob/658193537a640772633e656f4673334fe1644944/linit.go#L31-L42) libraries are loaded, in addition to [gopher-json](https://pkg.go.dev/layeh.com/gopher-json@v0.0.0-20201124131017-552bb3c4c3bf).
4. `snowplow_mode`: optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains a Lua table representation of the event - with keys as returned by the Snowplow Analytics SDK. 

```hcl
# lua configuration
transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICByZXR1cm4gaW5wdXQKZW5k"
    timeout_sec = 20
    sandbox     = true
    snowplow_mode = false
  }
}
```

## Examples

### Generic: Filter, transform and set destination partition key based on values in the data.

For this example, the input data is a json string which looks like this: 

// TODO: Tests and templating through this whole file.

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
    timeout_sec = 20
    sandbox     = false # Note that we set `sandbox` to false, since we utilise the preloaded `json` package provided by gopher-json.
    snowplow_mode = false
  }
}
```

#### Snowplow Events: Filter, transform and set destination partition key based on values in the data.


For this example, the input data is a valid Snowplow TSV event - so we can enable `snowplow_mode`, which will convert the data to a JSON before passing it to the script as a Lua table.

The script filters out non-web data, based on the `platform` value, otherwise it checks for a `user_id` value, setting a new `uid` field to that value if it's found, or `domain_userid` if not.

It also sets the partitionKey to `app_id`.

```lua
function main(input)
	-- input is a lua table
	local spData = input["Data"]
	if spData["platform"] ~= "web" then
	   return { FilterOut = true };
	end

	if spData["user_id"] ~= nil then
		spData["uid"] = spData["user_id"]
	else
		spData["uid"] = spData["domain_userid"]
	end
	return  { Data = spData, PartitionKey = spData["app_id"] }
end
```

The configuration for this script is:

```hcl
transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKCS0tIGlucHV0IGlzIGEgbHVhIHRhYmxlCglsb2NhbCBzcERhdGEgPSBpbnB1dFsiRGF0YSJdCglpZiBzcERhdGFbInBsYXRmb3JtIl0gfj0gIndlYiIgdGhlbgoJICAgcmV0dXJuIHsgRmlsdGVyT3V0ID0gdHJ1ZSB9OwoJZW5kCgoJaWYgc3BEYXRhWyJ1c2VyX2lkIl0gfj0gbmlsIHRoZW4KCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJ1c2VyX2lkIl0KCWVsc2UKCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJkb21haW5fdXNlcmlkIl0KCWVuZAoJcmV0dXJuICB7IERhdGEgPSBzcERhdGEsIFBhcnRpdGlvbktleSA9IGFwcF9pZCB9CmVuZA=="
    timeout_sec = 20
    snowplow_mode = true # Snowplow mode enabled - this transforms the tsv to a lua table
  }
}
```

