# Stream Replicator

[![Release][release-image]][releases]

## Overview

Easily replicate data streams wherever you need them to be!  This application is available in three different runtimes to facilitate different needs - AWS Lambda, GCP CloudFunctions and as a standalone application.

See the [wiki documention](https://github.com/snowplow-devops/stream-replicator/wiki) for details on how to configure and run the application.

## Quick start

Assuming git is installed:

```bash
 host> git clone https://github.com/snowplow-devops/stream-replicator
 host> cd stream-replicator
 host> make test
 host> make
```

To run integration tests:

```bash
 host> make integration-up # Sets up localstack ready for use
 host> make integration-test

 # Shutting localstack down
 host> make integration-down

 # Resetting localstack on any strange errors
 host> make integration-reset
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

### Publishing

This is handled through CI/CD on Github Actions. However all binaries will be generated by using the make command for local publishing and use.

### PROPRIETARY AND CONFIDENTIAL

Unauthorized copying of this project via any medium is strictly prohibited.

Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

[release-image]: http://img.shields.io/badge/golang-0.2.1-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow-devops/stream-replicator/releases/
