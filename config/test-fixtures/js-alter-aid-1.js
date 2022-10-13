function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "changed";
    return {
        Data: JSON.stringify(jsonObj)
    };
}