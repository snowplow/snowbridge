transform {
  use "js" {
    script_path = env.JS_HASH_AID_PATH
    hash_salt_secret = env.SHA1_SALT
  }
}
