# Snowbridge

[![Release][release-image]][releases]

## Overview

Snowbridge is a flexible, low latency tool which can replicate streams of data of any type to external destinations, optionally filtering or transforming the data along the way. It can be used to consume, transform and relay data from any supported source and to any supported destination, or any destination which supports HTTP.

See the [documention](https://docs.snowplow.io/docs/destinations/forwarding-events/snowbridge/) for details on how to configure and run the application.

## Testing

### Unit tests

Skips all tests that depend on docker resources

`go test ./... -short`

### Integration tests

Sets up local external resources and runs all integration tests against them.

1. `make integration-up`    - sets up docker containers required for integration tests
2. `make integration-test`  - runs integration tests
3. `make integration-down`  - brings docker containers down

### End to end tests

Sets up local external resources and runs pre-release tests using a fully built local docker image of the project.

1. `make all`      - build the project locally
2. `make e2e-up`   - sets up docker containers required for End-to-End tests
2. `make e2e-test` - runs End-to-End tests
3. `make e2e-down` - brings docker containers down

### LICENSE

Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.

The default distribution contains everything except for the Kinesis source, i.e. the ability to read from AWS Kinesis. This distribution is all licensed under the [Snowplow Limited Use License](https://docs.snowplow.io/limited-use-license-1.1/). _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions](https://docs.snowplow.io/docs/resources/limited-use-license-faq/).)_

The AWS-specific distribution contains everything, including the Kinesis source, i.e. the ability to read from AWS Kinesis. Like the default distribution, it’s licensed under the [Snowplow Limited Use License](https://docs.snowplow.io/limited-use-license-1.1/) ([frequently asked questions](https://docs.snowplow.io/docs/resources/limited-use-license-faq/)). However, this distribution has a dependency on [twitchscience/kinsumer](https://github.com/twitchscience/kinsumer), which is licensed by Twitch under the [Amazon Software License](https://github.com/twitchscience/kinsumer/blob/master/LICENSE).

To comply with the [Amazon Software License](https://github.com/twitchscience/kinsumer/blob/master/LICENSE), you may only use this distribution of Snowbridge _“with the web services, computing platforms or applications provided by Amazon.com, Inc. or its affiliates, including Amazon Web Services, Inc.”_

[release-image]: http://img.shields.io/badge/golang-3.2.3-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow/snowbridge/releases/
