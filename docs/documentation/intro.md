# Introduction

Stream Replicator is a flexible, low latency tool which can replicate streams of data of any type to external destinations, optionally filtering or transforming the data along the way. It can be used to consume, transform and relay data to any third party platform which supports HTTP or is listed as a target below - in real-time.

## Features:

- [Kinesis](https://aws.amazon.com/kinesis), [SQS](https://aws.amazon.com/sqs/), [PubSub](https://cloud.google.com/pubsub), and stdin sources

- [Kinesis](https://aws.amazon.com/kinesis), [SQS](https://aws.amazon.com/sqs/), [PubSub](https://cloud.google.com/pubsub), [Kafka](https://kafka.apache.org/), [EventHubs](https://azure.microsoft.com/en-us/services/event-hubs/), Http, and stdout targets.

- Custom in-flight Lua and JS transformations

- Low-latency Snowplow-specific data transformations

- Statsd and Sentry reporting and monitoring interfaces

## Snowplow

Stream Replicator is a generic tool, built to work on any type of data, developed by the Snowplow team. It began life as a closed-source tool developed to deliver various requirements related to Snowplow data, and so many of the features are specific to that data.

Snowplow 

// TODO: Blurb here...