function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "python-requests 2";
    jsonObj["app_id"] = hash(jsonObj["app_id"], "sha1")
    return {
        Data: JSON.stringify(jsonObj)
    };
}