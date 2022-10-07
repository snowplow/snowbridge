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