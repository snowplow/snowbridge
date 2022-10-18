function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "again";
    return {
        Data: JSON.stringify(jsonObj)
    };
}