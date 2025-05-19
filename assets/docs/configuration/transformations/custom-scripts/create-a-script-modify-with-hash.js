function main(input) {
    // input is a string, so we parse it
    var jsonObj = JSON.parse(input.Data);
    
    // we want to hash the name for security reason
    jsonObj.name = hash(jsonObj.name, "sha1")
    return {
        // Pass it back to Snowbridge via the Data field
        Data: jsonObj
    };
}