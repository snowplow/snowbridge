function main(input)
    -- input is a string, so we parse it
    local json = require("json")
    local jsonObj, _ = json.decode(input.Data)

    -- set the name field
    jsonObj.name = "Bruce Wayne"

    -- Pass it back to Stream Replicator via the Data field
    return { Data = jsonObj, ParititionKey = "myPk" }
end