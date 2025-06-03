function main(input) {
    var jsonObj = JSON.parse(input.Data);
    
    if (jsonObj.batmobileCount < 1) {
        return { FilterOut: true }
    }
    jsonObj.name = "Bruce Wayne"
    return {
        Data: jsonObj,
        PartitionKey: jsonObj.id }
}