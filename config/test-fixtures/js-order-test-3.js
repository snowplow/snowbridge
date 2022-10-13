function main(x) {
    var jsonObj = JSON.parse(x.Data);
    if (jsonObj["app_id"] == "2") {
        jsonObj["app_id"] = "3"
    }
    return {
        Data: JSON.stringify(jsonObj)
    };
}