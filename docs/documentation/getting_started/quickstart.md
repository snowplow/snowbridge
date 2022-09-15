# Getting Started

## Quickstart

The fastest way to get started and experiment with Stream Replicator is to run it via the command line:

1. Download the pre-compiled ZIP from the [releases](https://github.com/snowplow-devops/stream-replicator/releases/)
2. Unzip and run the binary with eg. `echo "hello world" | ./stream-replicator`

The defaults for the app are stdin source, no transformations, and stdout target - so this should print the message 'hello world' along with some logging data to the console.

Next, the app can be configured using HCL - simply create a configuration file, and provide the path to it using the `STREAM_REPLICATOR_CONFIG_FILE` environment variable. 

You can find a guide to configuration in the [configuration section](../configuration), and a guide to deployment in the [deployment page](./distribution-and-deployment.md)
