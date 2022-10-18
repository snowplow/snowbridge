function main(x) {
    var jsonObj = JSON.parse(x.Data);
    if (jsonObj["app_id"] == "1") {
        jsonObj["app_id"] = "2"
    }
    return {
        Data: JSON.stringify(jsonObj)
    };
}