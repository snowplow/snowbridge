function main(input)
	local json = require("json")
	local jsonObj, _ = json.decode(input.Data)
	if jsonObj.batmobileCount < 1 then 
		return {Data = "", FilterOut = true}
	end
	jsonObj.name = "Bruce Wayne"
	return { Data = jsonObj, PartitionKey = jsonObj.id }
end