function main(input) {
    // input is an object
    var spData = input.Data;
    return {
        Data: spData,
        PartitionKey: spData["event_id"]
    };
}