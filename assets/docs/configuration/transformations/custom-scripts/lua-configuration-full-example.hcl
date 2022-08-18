# lua configuration
transform {
  use "lua" {

    # Path to the script to be run.
    # The path provided must be relative to the runtime. 
    # When running the CLI directly, this is relative to the directory from which the cli is called - it is best to provide absolute paths in this instance.
    # When running via Docker, a file should be mounted to the container, and the path provided is the mount location.
    # For this example, we use an environment variable, to facilitate unit tests. A hardcoded value may also be provided (eg. "/tmp/myscript.lua")
    script_path = env.LUA_SCRIPT_PATH

    # Timeout for execution of the script, in seconds. (optional)
    timeout_sec = 20

    # if true, libraries are not opened by default. Otherwise, the default [gopher-lua](https://github.com/yuin/gopher-lua/blob/658193537a640772633e656f4673334fe1644944/linit.go#L31-L42) libraries are loaded, in addition to [gopher-json](https://pkg.go.dev/layeh.com/gopher-json).
    # Libraries are loaded on initialisation of the runtime - which is per-event. For better performance, set to true.
    sandbox     = true
  }
}