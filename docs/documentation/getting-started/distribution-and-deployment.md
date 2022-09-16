# Distribution and deployment

## Distribution options

Stream Replicator is available on docker:

```bash
docker pull snowplow/stream-replicator-aws:{version}
docker run snowplow/stream-replicator-aws:{version}
```

```bash
docker pull snowplow/stream-replicator-gcp:{version}
docker run snowplow/stream-replicator-gcp:{version}
```

Note that there are two versions of the build - `aws` may only be deployed to AWS services, due to the restrictive licence of the [kinsumer](https://github.com/twitchscience/kinsumer) package, which we use to consume from Kinesis.

The `gcp` build strips that source out, and so can be run on any platform.

## Deployment

The app can be deployed via services like EC2, ECS or Kubernetes using docker.

Configuration and authentication can be done by mounting the relevant files, and/or setting the relevant environment variables as per the standard authentication methods for cloud services.

