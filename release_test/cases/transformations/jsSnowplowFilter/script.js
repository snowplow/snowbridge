function main(input) {
// input is an object
    var spData = input.Data;
    if (!(spData["app_id"] && spData["app_id"].includes("aid_6"))) {
        return {
            FilterOut: true
        };
    }

    return input
}