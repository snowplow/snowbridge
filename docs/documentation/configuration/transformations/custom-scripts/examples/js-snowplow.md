# Javascript Example - Snowplow Data

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
    
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}
```
