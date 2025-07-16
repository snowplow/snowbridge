function main(x) {
    var jsonObj = JSON.parse(x.Data);
    return {
        Dat: JSON.stringify(jsonObj)
    };
}