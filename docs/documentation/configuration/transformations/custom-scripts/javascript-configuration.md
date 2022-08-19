# Custom Javascript Script Configuration

Scripts are provided to the configuration as base-64 encoded strings:

```hcl
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewogICAgcmV0dXJuIHsgRGF0YTogIkhlbGxvIFdvcmxkIiB9Cn0="
  }
}
```

Configuration options for JS scripting are:

// TODO: Replace this part with tested example config? Or leave until we've made that easier to do?

1. `source_b64`: required, the JS script encoded in B64. The scriptmust define a `main` function which takes one argument and returns an object mapping to engineProtocol.
2. `timeout_sec`: optional, the timeout in seconds of the JS script.
3. `disable_source_maps`: optional, disables JS source maps, which allow access to the filesystm. It is recommended to set to false where possible.
4. `snowplow_mode`: optional, may be used when the input is a Snowplow enriched TSV. This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.

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

## Examples

### Generic: Filter, transform and set destination partition key based on values in the data.

For this example, the input data is a json string which looks like this: 

// TODO: Add tests and templating/embedding

```json
{
  "name": "Bruce",
  "id": "b47m4n",
  "batmobileCount": 1
}
```

The script filters out any data with a `batmobileCount` less than 1, otherwise it updates the Data's `name` field to "Bruce Wayne", and sets the PartitionKey to the value of `id`:

```js
function main(input) {
    var jsonObj = JSON.parse(input.Data);
    
    if (jsonObj.batmobileCount < 1) {
        return { FilteredOut: true }
    }
    jsonObj.name = "Bruce Wayne"
    return {
        Data: jsonObj,
        PartitionKey: jsonObj.id
};
```

The configuration for this script is:

```hcl
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewoJCXZhciBqc29uT2JqID0gSlNPTi5wYXJzZShpbnB1dC5EYXRhKTsKCQkKCQlpZiAoanNvbk9iai5iYXRtb2JpbGVDb3VudCA8IDEpIHsKCQkJcmV0dXJuIHsgRmlsdGVyZWRPdXQ6IHRydWUgfQoJCX0KCQlqc29uT2JqLm5hbWUgPSAiQnJ1Y2UgV2F5bmUiCgkJcmV0dXJuIHsKCQkJRGF0YToganNvbk9iaiwKCQkJUGFydGl0aW9uS2V5OiBqc29uT2JqLmlkCgkJfTsKCSB9"
  }
}
```

### Snowplow Events: Filter, transform and set destination partition key based on values in the data.

For this example, the input data is a valid Snowplow TSV event - so we can enable `snowplow_mode`, which will convert the data to a JSON before passing it to the script as a JSON object.

The script below filters out non-web data, based on the `platform` value, otherwise it checks for a `user_id` value, setting a new `uid` field to that value if it's found, or `domain_userid` if not.

It also sets the partitionKey to `app_id`.

```js
function main(input) {
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
        Data: spData,
        PartitionKey: spData["app_id"]
    };
}
```     

The configuration for this script is:

```hcl
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewovLyBpbnB1dCBpcyBhbiBvYmplY3QKICAgIHZhciBzcERhdGEgPSBpbnB1dC5EYXRhOwoKICAgIGlmIChzcERhdGFbInBsYXRmb3JtIl0gIT0gIndlYiIpIHsKICAgICAgICByZXR1cm4gewogICAgICAgICAgICBGaWx0ZXJPdXQ6IHRydWUKICAgICAgICB9OwogICAgfQoKICAgIGlmICgidXNlcl9pZCIgaW4gc3BEYXRhKSB7CiAgICAgICAgc3BEYXRhWyJ1aWQiXSA9IHNwRGF0YVsidXNlcl9pZCJdCiAgICB9IGVsc2UgewogICAgICAgIHNwRGF0YVsidWlkIl0gPSBzcERhdGFbImRvbWFpbl91c2VyaWQiXQogICAgfQoKICAgIHJldHVybiB7CiAgICAgICAgRGF0YTogc3BEYXRhLAogICAgICAgIFBhcnRpdGlvbktleTogc3BEYXRhWyJhcHBfaWQiXQogICAgfTsKfQ=="
    timeout_sec         = 20
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}
```
