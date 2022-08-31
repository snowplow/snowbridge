# Getting Started

## Quickstart

The fastest way to get started and experiment with Stream Replicator is to run it via the command line:

1. Download the pre-compiled ZIP from the [releases][releases]
2. Unzip and run the binary with eg. `echo "hello world" | ./stream-replicator`

The defaults for the app are stdin source, no transformations, and stdout target - so this should print the message 'hello world' along with some logging data to the console.

// TODO: image here maybe?

Next, you can use a `.hcl` config file, or environment variables to configure different sources, targets and transformations.

// TODO: Should this line go here? If so, add a link!

See the reference section for the full list of configuration options, and the tutorials section for examples of implementing use cases.

## Distribution options

Stream replicator is available on docker:

`docker pull snowplow/stream-replicator-aws:{version}`
`docker pull snowplow/stream-replicator-gcp:{version}`

Or for download as a binary from Github [releases][releases]

// TODO: Does this bit need to be here?

Note that there are two versions of the build - `aws` may only be deployed to AWS services, due to the restrictive licence of the [kinsumer](https://github.com/twitchscience/kinsumer) package, which we use to consume from Kinesis.

The `gcp` build strips that source out, and so can be run on any platform.


## Deployment

The app can be deployed via services like EC2, ECS or Kubernetes using docker:

```bash
docker pull snowplow/stream-replicator-aws:{version}
docker run snowplow/stream-replicator-aws:{version}
```

```bash
docker pull snowplow/stream-replicator-gcp:{version}
docker run snowplow/stream-replicator-gcp:{version}
```

Configuration and authentication can be done by mounting the relevant files, and/or setting the relevant environment variables as per the standard authentication methods for cloud services.

[releases]: https://github.com/snowplow-devops/stream-replicator/releases/