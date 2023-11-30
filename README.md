# Snowbridge

[![Release][release-image]][releases]

## Overview

Snowbridge is a flexible, low latency tool which can replicate streams of data of any type to external destinations, optionally filtering or transforming the data along the way. It can be used to consume, transform and relay data from any supported source and to any supported destination, or any destination which supports HTTP.

See the [documention](https://docs.snowplow.io/docs/destinations/forwarding-events/snowbridge/) for details on how to configure and run the application.

### LICENSE

Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.

The default distribution contains everything except for the Kinesis source, i.e. the ability to read from AWS Kinesis. This distribution is all licensed under the [Snowplow Community License](https://docs.snowplow.io/community-license-1.0). _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions](https://docs.snowplow.io/docs/contributing/community-license-faq/).)_

The AWS-specific distribution contains everything, including the Kinesis source, i.e. the ability to read from AWS Kinesis. Like the default distribution, it’s licensed under the [Snowplow Community License](https://docs.snowplow.io/community-license-1.0) ([frequently asked questions](https://docs.snowplow.io/docs/contributing/community-license-faq/)). However, this distribution has a dependency on [twitchscience/kinsumer](https://github.com/twitchscience/kinsumer), which is licensed by Twitch under the [Amazon Software License](https://github.com/twitchscience/kinsumer/blob/master/LICENSE).

To comply with the [Amazon Software License](https://github.com/twitchscience/kinsumer/blob/master/LICENSE), you may only use this distribution of Snowbridge _“with the web services, computing platforms or applications provided by Amazon.com, Inc. or its affiliates, including Amazon Web Services, Inc.”_

[release-image]: http://img.shields.io/badge/golang-2.2.2-6ad7e5.svg?style=flat
[releases]: https://github.com/snowplow/snowbridge/releases/
