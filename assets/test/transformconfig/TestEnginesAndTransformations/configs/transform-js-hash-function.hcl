transform {
  use "js" {
    script = <<JSEOT
    function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "python-requests 2";
    jsonObj["app_id"] = hash(jsonObj["app_id"], "sha1")
    return {
        Data: JSON.stringify(jsonObj)
    };
}
JSEOT
    hash_salt_secret = env.SHA1_SALT
  }
}
