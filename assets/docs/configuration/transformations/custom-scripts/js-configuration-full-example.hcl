# transform configuration
transform {
  use "js" {

    # Path to the script to be run.
    # Either script_path or script may be provided. If both are provided, script_path is used.
    # The path provided must be relative to the runtime. 
    # When running the CLI directly, this is relative to the directory from which the cli is called - it is best to provide absolute paths in this instance.
    # When running via Docker, a file should be mounted to the container, and the path provided is the mount location.
    # For this example, we use an environment variable, to facilitate unit tests. A hardcoded value may also be provided (eg. "/tmp/myscript.js")
    script_path = env.JS_SCRIPT_PATH

    # Literal JS script string
    # Either script_path or script may be provided. If both are provided, script_path is used.
    script = "func main(input) { return input }"

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec         = 20

    # optional, may be used when the input is a Snowplow enriched TSV. 
    # This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode       = true

    # optional, removes null or empty keys from JSON object output of transformation. Only applicable when the script returns an object. Does not remove null or empty elements of arrays.
    remove_nulls = true

    # optional, allows to pass salt value into `hash` function in scripts
    hash_salt_secret = env.SHA1_SALT
  }
}