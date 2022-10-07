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