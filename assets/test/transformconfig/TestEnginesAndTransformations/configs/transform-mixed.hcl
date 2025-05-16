transform {
  use "js" {
    script = <<JSEOT
    function main(x) {
    var jsonObj = JSON.parse(x.Data);
    jsonObj["app_id"] = "changed";
    return {
        Data: JSON.stringify(jsonObj)
    };
}
JSEOT
  }
}

transform {
  use "js" {
    script_path = env.JS_ALTER_AID_2_PATH
  }
}
