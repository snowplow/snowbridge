# Stream Replicator

[![Release][release-image]](releases)

## Overview

Easily replicate data streams wherever you need them to be!  This application is available in three different runtimes to facilitate different needs - AWS Lambda, GCP CloudFunctions and as a standalone application.

## Quick start

Assuming git is installed:

```bash
 host> git clone https://github.com/snowplow-devops/stream-replicator
 host> cd stream-replicator
 host> make test
 host> make
```

All compiled assets are available under `build/compiled`.

To remove all build files:

```bash
 host> make clean
```

To format the golang code in the source directory:

```bash
 host> make format
```

**Note:** Always run `make format` before submitting any code.

**Note:** The `make test` command also generates a code coverage file which can be found at `build/coverage/coverage.html`.

## Targets & Configuration

Currently supported targets are:

1. `stdout`: Allows for easy debugging
2. `kinesis`: Replicates to an Amazon Kinesis Stream
3. `pubsub`: Replicates to a GCP PubSub Topic

All configuration for this application is done via Environment variables.  There are no config files.  Depending on the `TARGET` you define you will need to populate the associated variables - for example to send to Kinesis you need to fill out the `KINESIS_` variables.

| Variable                            | Possible Value(s)                           | Note                                                                  |
|-------------------------------------|---------------------------------------------|-----------------------------------------------------------------------|
| `TARGET`                            | `stdout, kinesis, pubsub`                   | Sets the target stream to emit data into `(def: "stdout")`            |
| `LOG_LEVEL`                         | `debug, info, warning, error, fatal, panic` | Sets the logging level `(def: "info")`                                |
| `SENTRY_DSN`                        | `https://acme.com/1`                        | The DSN to send Sentry alerts to `(def: "")`                          |
| `SENTRY_TAGS`                       | `{\"client_name\":\"com.acme\"}`            | Escaped JSON string with tags to send to sentry `(def: "{}")`         |
| `SENTRY_DEBUG`                      | `true, false`                               | Whether to put Sentry into debug mode `(def: "false")`                |
| `TARGET_KINESIS_STREAM_NAME`        | `some-acme-stream`                          | Name of the stream to send data into `(def: "")`                      |
| `TARGET_KINESIS_REGION`             | `us-east-1`                                 | The region the stream is in `(def: "")`                               |
| `TARGET_KINESIS_ROLE_ARN`           | `arn:aws:iam::111111111111:role/Kinesis`    | *Optional* IAM role to assume `(def: "")`                             |
| `TARGET_PUBSUB_PROJECT_ID`          | `acme-project`                              | ID of the GCP Project `(def: "")`                                     |
| `TARGET_PUBSUB_TOPIC_NAME`          | `some-acme-topic`                           | Name of the topic to send data into `(def: "")`                       |
| `TARGET_PUBSUB_SERVICE_ACCOUNT_B64` | `asdasdasdasdasd=`                          | *Optional* GCP Service Account Base64 encoded `(def: "")`             |

## Serverless runtimes

### AWS: Lambda (Kinesis)

The Lambda deployment allows you to replicate a Kinesis stream to any of the available targets. To deploy the Lambda:

1. Download the pre-compiled ZIP from the [releases](releases) or read the Quick start to compile from source (the ZIP is available locally at `build/compiled/aws_lambda_stream_replicator_${version}_linux_amd64.zip`).
2. Setup an IAM role with sufficient Kinesis + Logging permissions - the following default roles can be used for testing:
 - `arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`
 - `arn:aws:iam::aws:policy/AmazonKinesisFullAccess`
3. Create a new function:
  - Give it a name (e.g. `stream-replicator`)
  - Select the `Go 1.x` runtime
  - Set the role you just created in the `Permissions` dropdown
4. Upload the function code from the ZIP file (`Actions -> Upload a .zip file`)
5. Set runtime settings and change the `Handler` from `hello` -> `HandleRequest`
6. Set the environment variables (see Configuration above for details)

You are now ready to `Add trigger` for the function!  Simply select Kinesis and the stream you wish to replicate and set to enable!

### GCP: CloudFunctions (PubSub)

The CloudFunctions deployment allows you to replicate a PubSub topic to any of the available targets. To deploy the CloudFunction:

1. Download the pre-compiled ZIP from the [releases](releases) or read the Quick start to compile from source (the ZIP is available locally at `build/compiled/gcp_cloudfunctions_stream_replicator_${version}_linux_amd64.zip`).
2. Create a new function:
  - Give it a name (e.g. `stream-replicator`)
  - Select a region
  - Set `Trigger type` -> `Cloud Pub/Sub`
  - Select the source topic to pull from
3. Set the runtime environment variables (see Configuration above for details)
4. Set `Runtime` -> `Go 1.13`
5. Set `Entry point` -> `HandleRequest`
6. For `Source code` select ZIP upload and upload the function code

Hit deploy and the stream should start to replicate!

## Standalone CLI (*Warning*: Advanced mode!)

If running the replicator via Lambda / CloudFunctions is not possible or not feasible for performance (or other) reasons then it can be run directly as a standalone application.  This mode supports consuming from three different sources:

1. `stdin`: Will allow you to stream arbitrary text in from your local system or even a whole file (e.g. `cat file.txt | ./stream-replicator`)
2. `kinesis`: Allows you to consume a Kinesis stream across multiple replicator instances concurrently
3. `pubsub`: Allows you to consume a PubSub topic across multiple replicator instances concurrently

To configure these sources several extra configuration variables are required - as with the targets everything is done via the environment!

| Variable                            | Possible Value(s)                           | Note                                                                  |
|-------------------------------------|---------------------------------------------|-----------------------------------------------------------------------|
| `SOURCE`                            | `stdin, kinesis, pubsub`                    | Sets the target stream to pull data from `(def: "stdin")`             |
| `SOURCE_KINESIS_STREAM_NAME`        | `some-acme-stream`                          | Name of the stream to pull data from `(def: "")`                      |
| `SOURCE_KINESIS_REGION`             | `us-east-1`                                 | The region the stream is in `(def: "")`                               |
| `SOURCE_KINESIS_ROLE_ARN`           | `arn:aws:iam::111111111111:role/Kinesis`    | *Optional* IAM role to assume `(def: "")`                             |
| `SOURCE_KINESIS_APP_NAME`           | `acme-stream-replicator`                    | Application name to use for DynamoDB checkpointing tables `(def: "")` |
| `SOURCE_PUBSUB_PROJECT_ID`          | `acme-project`                              | ID of the GCP Project `(def: "")`                                     |
| `SOURCE_PUBSUB_SUBSCRIPTION_ID`     | `some-acme-topic`                           | Name of the subscription to pull data from `(def: "")`                |
| `SOURCE_PUBSUB_SERVICE_ACCOUNT_B64` | `asdasdasdasdasd=`                          | *Optional* GCP Service Account Base64 encoded `(def: "")`             |

When ready the steps are as follows:

1. Download the pre-compiled ZIP from the [releases](releases) or read the Quick start to compile from source (the ZIP is available locally at `build/compiled/cli_stream_replicator_${version}_linux|darwin_amd64.zip`).
2. Set the appropriate env vars as noted above (see `SOURCE` specific settings below)
3. Run by launching the binary `e.g. ./stream-replicator`; this is a long running operation and will only stop on a `ctrl + c` command or on an error

### AWS: Kinesis

To consume from Kinesis you will need to:

1. Configure the above required `SOURCE_KINESIS_*` variables
2. Create three DynamoDB tables which will be used for checkpointing the progress of the replicator on the stream (*Note*: details below)

| TableName                                | DistKey        |
|------------------------------------------|----------------|
| `${SOURCE_KINESIS_APP_NAME}_clients`     | ID (String)    |
| `${SOURCE_KINESIS_APP_NAME}_checkpoints` | Shard (String) |
| `${SOURCE_KINESIS_APP_NAME}_metadata`    | Key (String)   |

Assuming your AWS credentials have sufficient permission to Kinesis and DynamoDB your consumer should now be able to run by launching the executable.

_WARNING_: This consumer always starts from `TRIM_HORIZON` - be mindful of this when launching.

### GCP: PubSub

To consume from PubSub you will need to create a subscription for the topic you wish to consume from first.  Once created you can define it and start consuming from PubSub.

### Publishing

This is handled through CI/CD on Github Actions. However all binaries will be generated by using the make command for local publishing and use.

### PROPRIETARY AND CONFIDENTIAL

Unauthorized copying of this project via any medium is strictly prohibited.

Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

[release-image]: http://img.shields.io/badge/golang-0.1.0-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow-devops/stream-replicator/releases
