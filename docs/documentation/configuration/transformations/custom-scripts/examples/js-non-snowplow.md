# Javascript Example - Non-Snowplow Data

For this example, the input data is a json string which looks like this: 

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
        PartitionKey: jsonObj.id }
}
```

The configuration for this script is:

```hcl
transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewoJCXZhciBqc29uT2JqID0gSlNPTi5wYXJzZShpbnB1dC5EYXRhKTsKCQkKCQlpZiAoanNvbk9iai5iYXRtb2JpbGVDb3VudCA8IDEpIHsKCQkJcmV0dXJuIHsgRmlsdGVyZWRPdXQ6IHRydWUgfQoJCX0KCQlqc29uT2JqLm5hbWUgPSAiQnJ1Y2UgV2F5bmUiCgkJcmV0dXJuIHsKCQkJRGF0YToganNvbk9iaiwKCQkJUGFydGl0aW9uS2V5OiBqc29uT2JqLmlkCgkJfTsKCSB9"
  }
}
```