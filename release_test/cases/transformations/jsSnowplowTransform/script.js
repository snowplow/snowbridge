function main(input) {
    // input is an object
    var spData = input.Data;

    spData["app_id"] = "test"

    return {
        Data: spData
    };
}