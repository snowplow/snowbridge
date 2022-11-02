function main(input) {
    // input is a string, so we parse it
    var jsonObj = JSON.parse(input.Data);
    
    // set the name field
    jsonObj.name = "Bruce Wayne"
    return {
        // Pass it back to Snowbridge via the Data field
        Data: jsonObj,
        PartitionKey: "myPk"
    };
}