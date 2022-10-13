function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "1";
    return {
        Data: JSON.stringify(jsonObj)
    };
}