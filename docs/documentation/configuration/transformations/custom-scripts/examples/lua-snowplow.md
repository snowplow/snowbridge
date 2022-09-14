# Lua Example - Snowplow Data

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

    snowplow_mode = true # Snowplow mode enabled - this transforms the tsv to a lua table
  }
}
```