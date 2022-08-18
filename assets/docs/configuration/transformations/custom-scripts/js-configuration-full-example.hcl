# transform configuration
transform {
  use "js" {

    # Path to the script to be run.
    # The path provided must be relative to the runtime. 
    # When running the CLI directly, this is relative to the directory from which the cli is called - it is best to provide absolute paths in this instance.
    # When running via Docker, a file should be mounted to the container, and the path provided is the mount location.
    # For this example, we use an environment variable, to facilitate unit tests. A hardcoded value may also be provided (eg. "/tmp/myscript.js")
    script_path = env.JS_SCRIPT_PATH

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec         = 20

    # optional, may be used when the input is a Snowplow enriched TSV. 
    # This will transform the data so that the `Data` field contains an object representation of the event - with keys as returned by the Snowplow Analytics SDK.
    snowplow_mode       = true
  }
}