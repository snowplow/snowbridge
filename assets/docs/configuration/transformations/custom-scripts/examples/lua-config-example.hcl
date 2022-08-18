transform {
  use "lua" {

    # Path to the script to be run.
    # The path provided must be relative to the runtime. 
    # When running the CLI directly, this is relative to the directory from which the cli is called - it is best to provide absolute paths in this instance.
    # When running via Docker, a file should be mounted to the container, and the path provided is the mount location.
    # For this example, we use an environment variable, to facilitate unit tests. A hardcoded value may also be provided (eg. "/tmp/myscript.lua")
    script_path = env.LUA_SCRIPT_EXAMPLE_PATH

    sandbox     = false # This setting preloads the json package, along with some other default packages
  }
}