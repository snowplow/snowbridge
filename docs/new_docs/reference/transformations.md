Note that in stream replicator, there aren't two distinct concepts for filters and transformations - conceptually, a filter is a type of transformation. In the below section we distinguish the two for categorisation purposes, but in configuration, filter names are transformation names, and filters are provided via `transform {}` blocks.

Transformations operate on a per-event basis, can be chained (ie executed one after the other), and will be evaluated in the order provided in the configuration.

The order in which transformations are provided is matters to performance in two ways: 

Firstly, filters should come first where possible, since we then avoid additional processing overhead on data which is filtered out.

Secondly, where possible, if there are more than one of `sp`, `js` or `lua` transformations respectively, they should be provided next to each other, where possible. This is because under the hood, these transformations will share data with each other, which allows the app to save on re-doing operations (for example parsing a Snowplow tsv event more than once).

Currently supported transformation names are:

1. `spEnrichedToJson`
2. `spEnrichedSetPk`
3. `spEnrichedFilter`
4. `spEnrichedFilterContext`
5. `spEnrichedFilterUnstructEvent` 
6. `js`
7. `lua`

## Configuration via file

To configure transformations via HCL file, supply one or more `transform {}` block. Choose the transformation using `use "{transformation_name}"`. Accepted arguments for each transformations `use` block are detailed below.

Example:

The below first filters out any `event_name` which does not match the regex `^page_view$`, then runs a custom javascript script to change the app_id value to `"1"`

```hcl
transform {
  use "spEnrichedFilter" {
    # keep only page views
    atomic_field = "event_name"
    regex = "^page_view$"
  }
}

transform {
  use "js" {
    # changes app_id to "1"
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KSB7CiAgICB2YXIganNvbk9iaiA9IEpTT04ucGFyc2UoeC5EYXRhKTsKICAgIGpzb25PYmpbImFwcF9pZCJdID0gIjEiOwogICAgcmV0dXJuIHsKICAgICAgICBEYXRhOiBKU09OLnN0cmluZ2lmeShqc29uT2JqKQogICAgfTsKfQ=="
  }
}
```

## Configuration via environment variables

If configuring via environment variable, one must create a hcl configuration as described in the section above, base-64 encode it, then provide that encoded value to `TRANSFORM_CONFIG_B64`.

Example:

The same configuration as above is provided as:

```bash
export TRANSFORM_CONFIG_B64="dHJhbnNmb3JtIHsKICB1c2UgInNwRW5yaWNoZWRGaWx0ZXIiIHsKICAgICMga2VlcCBvbmx5IHBhZ2Ugdmlld3MKICAgIGF0b21pY19maWVsZCA9ICJldmVudF9uYW1lIgogICAgcmVnZXggPSAiXnBhZ2VfdmlldyQiCiAgfQp9Cgp0cmFuc2Zvcm0gewogIHVzZSAianMiIHsKICAgICMgY2hhbmdlcyBhcHBfaWQgdG8gIjEiCiAgICBzb3VyY2VfYjY0ID0gIlpuVnVZM1JwYjI0Z2JXRnBiaWg0S1NCN0NpQWdJQ0IyWVhJZ2FuTnZiazlpYWlBOUlFcFRUMDR1Y0dGeWMyVW9lQzVFWVhSaEtUc0tJQ0FnSUdwemIyNVBZbXBiSW1Gd2NGOXBaQ0pkSUQwZ0lqRWlPd29nSUNBZ2NtVjBkWEp1SUhzS0lDQWdJQ0FnSUNCRVlYUmhPaUJLVTA5T0xuTjBjbWx1WjJsbWVTaHFjMjl1VDJKcUtRb2dJQ0FnZlRzS2ZRPT0iCiAgfQp9"
```

## Predefined transformations

All `sp` transformations must take a valid Snowplow enriched tsv event as input.

### Snowplow-specific transformations

1. `spEnrichedToJson`: Transforms a message's data from Snowplow Enriched tsv string format to a JSON object

spEnrichedToJson requires no further configuration.

Example:

```hcl
transform {
  use "spEnrichedToJson" {
  }
}
```

2. `spEnrichedSetPk`: Sets the message partition key to an atomic field from a Snowplow Enriched tsv string.

Example:

```hcl
transform {
  use "spEnrichedSetPk" {
    atomic_field = "app_id"
  }
}
```

Note: currently, setting partition key to fields in custom events and contexts is unsupported.

### Snowplow-specific filters

It is advised to run filters first, to avoid unnecessary processing of data. Predefined filters may be chained one-after-another, but may not be combined in such a way as one filter is aware of the other. Any more complex or nuanced logic may be achieved via scripting transformations.

For all regex-based filters, matches are kept, and messages which don't match are acked immediately, but not sent to the target.

1. `spEnrichedFilter`: Filters messages based on a regex match against an atomic field

This transformation is for use on base-level atomic fields, rather than fields from contexts, or custom events.

Example:

```hcl
transform {
  use "spEnrichedFilter" {
    atomic_field = "platform"
    regex = "web|mobile"
    regex_timeout = 10
  }
}
```

2. `spEnrichedFilterContext`: Filters messages based on a regex match against a field in a context.

This transformation is for use on fileds from contexts.

Note that if the same context is provided more than once, one instance of a match is enough for the regex condition to be considered a match - and the message to be kept.

The full parsed context name must be provided, in camel case, in the format returned by the Snowplow analytics SDK: `contexts_{vendor}_{name}_{major version}` - for example `contexts_nl_basjes_yauaa_context_1`.

The path to the field to be matched must then be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

Example:

```hcl
transform {
  use "spEnrichedFilterContext" {
    context_full_name = "contexts_nl_basjes_yauaa_context_1" 
    custom_field_path = "test1.test2[0]"
    regex = "^((?!test-data).)*$"
    regex_timeout = 10
  }
}
```

3. `spEnrichedFilterUnstructEvent`: Filters messages based on a regex match against a field in a custom event.

This transformatino is for use on fields from custom events.

The event name must be provided as it appears in the `event_name` field of the event (eg. `add_to_cart`). 

Optionally, a regex can be provided to match against the stringified version of the event (eg. `1-*-*`)

The path to the field to match against must be provided as a jsonpath (dot notation and square braces only) - for example `test1.test2[0].test3`.

Example:

```hcl
transform {
  use "spEnrichedFilterUnstructEvent" {
    unstruct_event_name = "unstruct_event_add_to_cart"
    custom_field_path = "sku"
    regex = "test-data"
    regex_timeout = 10
  }
}
```
## Javascript and Lua transformations

Custom tranformation scripts may be defined in Javascript or Lua, and provided to stream replicator.

For the limited benchmarks carried out, the Lua transformation offers a slight performance benefit over Javascript, however both are significantly less performant than the predefined transformations above (as you would expect).

### The scripting interface

The script - whether Lua or Javascript - must define a main function with a single argument. Stream replicator will pass an engineProtocol-like data structure as the argument:



The interface

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

#### Accessing data

Scripts can access the message Data at `input.Data`, and can return modified data by returning it in the `Data` field of the output. Likewise for the partition key - `input.PartitionKey` and the `PartitionKey` field of the output.

By default, the Data field will be a string. For Snowplow enriched TSV data, the `snowplow_mode` option transforms the data to an object for Javascript, or a table for Lua - field names will be those output by the Snowplow Analytics SDK.

The output of the script must be an object (Javascript) or a table (Lua) which maps to engineProtocol.

If the `FilterOut` field of the output is returned as `true`, the message will be acked immediately and won't be sent to the target. This will be the behaviour regardelss of what is returned to the other fields in the protocol.

```js
function main(input) {
    return { FilterOut: true }
}
```

```lua
function main(input)
	local t = { FilteredOut = true, Data = ""}
	return t
end
```

Returning a `PartitionKey` field is always optional, if returned it will set the partition key for the message.

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

The `Data` field returned may be either a string, or an object (Javascript) / table (Lua).

### Configuration:

Scripts are provided to the configuration as base-64 encoded strings:

```hcl
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="
  }
}

transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICByZXR1cm4gaW5wdXQKZW5k"
  }
}
```

#### Javascript

Configuration options for JS scripting are:

1. `source_b64`: required, the JS script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol.
2. `timeout_sec`: optional, the timeout in seconds of the JS script.
3. `disable_source_maps`: optional, disables JS source maps, which allow access to the filesystm. It is recommended to set to false where possible.
4. `snowplow_mode`: optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.

Example:

```hcl
# transform configuration
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="
    timeout_sec         = 20
    disable_source_maps = true
    snowplow_mode       = false
  }
}
```

#### Lua

Configuration options for Lua scripting are:

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

### Example use cases

#### Generic: Filter, transform and set partition key based on values in the data.

For this example, the input data is a json string which looks like this: `{"name": "Bruce", "id": "b47m4n", "batmobileCount": 1}`.

The script filters out any data with a `batmobileCount` less than 1, otherwise it updates the Data's `name` field to "Bruce Wayne", and sets the PartitionKey to the value of `id`:

<details>
<summary>Lua</summary>
<summary>Script</summary>
<pre><code>function main(input)
	local json = require("json")
	local jsonObj, _ = json.decode(input.Data)
	if jsonObj.batmobileCount < 1 then 
		return {Data = "", FilterOut = true}
	end
	jsonObj.name = "Bruce Wayne"
	return { Data = jsonObj, PartitionKey = jsonObj.id }
  end</code></pre>
<summary>Configuration</summary>
<pre><code>transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbih4KQoJbG9jYWwganNvbiA9IHJlcXVpcmUoImpzb24iKQoJbG9jYWwganNvbk9iaiwgXyA9IGpzb24uZGVjb2RlKHguRGF0YSkKCWlmIGpzb25PYmouYmF0bW9iaWxlQ291bnQgPCAxIHRoZW4gCgkJcmV0dXJuIHtEYXRhID0gIiIsIEZpbHRlck91dCA9IHRydWV9CgllbmQKCWpzb25PYmoubmFtZSA9ICJCcnVjZSBXYXluZSIKCXJldHVybiB7IERhdGEgPSBqc29uT2JqLCBQYXJ0aXRpb25LZXkgPSBqc29uT2JqLmlkIH0KICBlbmQ="
    timeout_sec = 20
    sandbox     = false # Note that we set `sandbox` to false, since we utilise the preloaded `json` package provided by gopher-json.
    snowplow_mode = false
  }
}</code></pre>
</details>


<details>
<summary>JS</summary>
<summary>Script</summary>
<pre><code>function main(input) {
		var jsonObj = JSON.parse(input.Data);
		
		if (jsonObj.batmobileCount < 1) {
			return { FilteredOut: true }
		}
		jsonObj.name = "Bruce Wayne"
		return {
			Data: jsonObj,
			PartitionKey: jsonObj.id
		};
	 }
   </code></pre>
<summary>Configuration</summary>
<pre><code>transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewoJCXZhciBqc29uT2JqID0gSlNPTi5wYXJzZShpbnB1dC5EYXRhKTsKCQkKCQlpZiAoanNvbk9iai5iYXRtb2JpbGVDb3VudCA8IDEpIHsKCQkJcmV0dXJuIHsgRmlsdGVyZWRPdXQ6IHRydWUgfQoJCX0KCQlqc29uT2JqLm5hbWUgPSAiQnJ1Y2UgV2F5bmUiCgkJcmV0dXJuIHsKCQkJRGF0YToganNvbk9iaiwKCQkJUGFydGl0aW9uS2V5OiBqc29uT2JqLmlkCgkJfTsKCSB9"
    timeout_sec         = 20
    disable_source_maps = true
    snowplow_mode       = false
  }
}</code></pre>
</details>

#### Snowplow Events: Filter, transform and set partition key based on values in the data.


For this example, the input data is a valid Snowplow TSV event.

The script filters out non-web data, based on the `platform` value, otherwise it checks for a `user_id` value, setting a new `uid` field to that value if it's found, or `domain_userid` if not.

It also sets the partitionKey to `app_id`.


<details>
<summary>Lua</summary>
<summary>Script</summary>
<pre><code>function main(input)
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
	return  { Data = spData, PartitionKey = app_id }
end</code></pre>
<summary>Configuration</summary>
<pre><code>transform {
  use "lua" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKCS0tIGlucHV0IGlzIGEgbHVhIHRhYmxlCglsb2NhbCBzcERhdGEgPSBpbnB1dFsiRGF0YSJdCglpZiBzcERhdGFbInBsYXRmb3JtIl0gfj0gIndlYiIgdGhlbgoJICAgcmV0dXJuIHsgRmlsdGVyT3V0ID0gdHJ1ZSB9OwoJZW5kCgoJaWYgc3BEYXRhWyJ1c2VyX2lkIl0gfj0gbmlsIHRoZW4KCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJ1c2VyX2lkIl0KCWVsc2UKCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJkb21haW5fdXNlcmlkIl0KCWVuZAoJcmV0dXJuICB7IERhdGEgPSBzcERhdGEsIFBhcnRpdGlvbktleSA9IGFwcF9pZCB9CmVuZA=="
    timeout_sec = 20
    sandbox     = true 
    snowplow_mode = true # Snowplow mode enabled - this transforms the tsv to a lua table
  }
}</code></pre>
</details>



<details>
<summary>JS</summary>
<summary>Script</summary>
<pre><code>function main(input) {
		// input is an object
		var spData = input.Data;
		
		if (spData["platform"] != "web") {
			return {
				FilterOut: true
			};
		}

		if ("user_id" in spData) {
			spData["uid"] = spData["user_id"]
		} else {
			spData["uid"] = spData["domain_userid"]
		}

		return {
			Data: spData
		};
	 }
   </code></pre>
<summary>Configuration</summary>
<pre><code>transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewoJCS8vIGlucHV0IGlzIGFuIG9iamVjdAoJCXZhciBzcERhdGEgPSBpbnB1dC5EYXRhOwoJCQoJCWlmIChzcERhdGFbInBsYXRmb3JtIl0gIT0gIndlYiIpIHsKCQkJcmV0dXJuIHsKCQkJCUZpbHRlck91dDogdHJ1ZQoJCQl9OwoJCX0KCgkJaWYgKCJ1c2VyX2lkIiBpbiBzcERhdGEpIHsKCQkJc3BEYXRhWyJ1aWQiXSA9IHNwRGF0YVsidXNlcl9pZCJdCgkJfSBlbHNlIHsKCQkJc3BEYXRhWyJ1aWQiXSA9IHNwRGF0YVsiZG9tYWluX3VzZXJpZCJdCgkJfQoKCQlyZXR1cm4gewoJCQlEYXRhOiBzcERhdGEKCQl9OwoJIH0="
    timeout_sec         = 20
    disable_source_maps = true
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object
  }
}</code></pre>
</details>

