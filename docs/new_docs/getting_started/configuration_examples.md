# Configuration examples

## Configure a source

The following hcl block configures a source to read from `mySqsQueue`.

```
source {
  use "sqs" {
    # SQS queue name
    queue_name = "mySqsQueue"

    # AWS region of SQS queue
    region     = "us-west-1"
  }
}

```

Assuming you have already downloaded the relevant binary as per the quickstart:

// TODO: LINK TO QUICKSTART

To use this configuration to read from an sqs queue, and output the data to stdout:

1. Copy the above into a configuration file with extension `.hcl`, and replace the values in this configuration, leaving all other configuration options as the default.
2. Set the `STREAM_REPLICATOR_CONFIG_FILE` environment variable to the path to your file, and set the AWS authentication env vars.
3. Run the binary with `./stream-replicator`.

Your stream's data should now be output to the console, along with logging data. Note that the application will continue to either read data from the sqs queue or wait for new data until you exit.


## Configure a target

The following hcl block configures stream replicator to output to a pubsub target.


```hcl
target {
  use "pubsub" {
    # ID of the GCP Project
    project_id = "acme-project"

    # Name of the topic to send data into
    topic_name = "some-acme-topic"
  }
}
```

Assuming you have already downloaded the relevant binary as per the quickstart:

// TODO: LINK TO QUICKSTART

To use this configuration to write data from a local file (json for example) - via stdin - to pubsub:

1. Copy the above into a configuration file with extension `.hcl`, and replace the values in this configuration, leaving all other configuration options as the default.
2. Set the `STREAM_REPLICATOR_CONFIG_FILE` environment variable to the path to your file
3. Run the binary with `my-data.json | ./stream-replicator`

Your file's data will be sent to pubsub, and the app will exit once the input is exhausted.


## Configure transformations

For a full explanation of the scripting interface and configuration, see the reference docs for transformations.

// TODO: link reference


### Scripting transformations - Non-Snowplow data

For this example, the input data is a json string which looks like this: 

```json
{
  "name": "Bruce",
  "id": "b47m4n",
  "batmobileCount": 1
}
```

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



### Built-in transformations - Snowplow data

For these examples, the input data is a valid Snowplow TSV event. A range of built-in basic transformations for Snowplow data are available - these are far more efficient and performant than the scripting layer.

The follwing configuration will first set up a filter to exclude events with `test` app IDs, then a filter to keep only `page_view` or `sceen_view` event names, then it will set the destination's partition key to the value of network_userid, then it will transform the event to JSON (using the Snowplow analytics SDK). // TODO: link I guess

```hcl
transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^(?!test).*$"
  }
}

transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^(page_view|screen_view)$"
  }
}

transform {
  use "spEnrichedFilter" {
    atomic_field = "app_id"
    regex = "^(page_view|screen_view)$"
  }
}

transform {
  use "spEnrichedSetPk" {
    atomic_field = "network_userid"
  }
}

transform {
  use "spEnrichedToJson" {
  }
}
```

### Scriptingg transformations - Snowplow data

For this example, the input data is a valid Snowplow TSV event.

The built-in trnasformations are a means of performing basic transformations in a fast and efficient manner. Where the requirement is not possible using those basic transformations, you might decide to instrument a scripting transformation for Snowplow data.

Some of the work is done for you - when `snowplow_mode` is set to `true`, stream-replicator will pre-transform the data into a JS object or a Lua table before passing it to the script.

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
