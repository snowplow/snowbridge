transform {
  use "js" {
    # Path to the script to be run.
    # The path provided must be relative to the runtime. 
    # When running the CLI directly, this is relative to the directory from which the cli is called - it is best to provide absolute paths in this instance.
    # When running via Docker, a file should be mounted to the container, and the path provided is the mount location.
    # For this example, we use an environment variable, to facilitate unit tests. A hardcoded value may also be provided (eg. "/tmp/myscript.js")
    script_path = env.JS_SNOWPLOW_SCRIPT_PATH

    # If the data is in JSON format, then it would be converted into an object to use in JS script
    # If the data is not in JSON format, transformation would fail
    json_mode = true # If true, then overrides snowplow_mode
  }
}