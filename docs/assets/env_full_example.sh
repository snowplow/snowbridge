#!/bin/sh

# source
export SOURCE_NAME="kinesis"                                    \
SOURCE_KINESIS_STREAM_NAME="my-stream"                          \
SOURCE_KINESIS_REGION="us-west-1"                               \
SOURCE_KINESIS_APP_NAME="StreamReplicatorProd1"                 \
SOURCE_KINESIS_ROLE_ARN="arn:aws:iam::123456789012:role/myrole" \
SOURCE_KINESIS_START_TSTAMP="2020-01-01 10:00:00"               \
SOURCE_CONCURRENT_WRITES=15

# transformations
export TRANSFORM_CONFIG_B64="dHJhbnNmb3JtIHsKICB1c2UgInNwRW5yaWNoZWRGaWx0ZXIiIHsKICAgICMga2VlcCBvbmx5IHBhZ2Ugdmlld3MKICAgIGF0b21pY19maWVsZCA9ICJldmVudF9uYW1lIgogICAgcmVnZXggPSAiXnBhZ2VfdmlldyQiCiAgfQp9Cgp0cmFuc2Zvcm0gewogIHVzZSAianMiIHsKICAgICMgY2hhbmdlcyBhcHBfaWQgdG8gIjEiCiAgICBzb3VyY2VfYjY0ID0gIlpuVnVZM1JwYjI0Z2JXRnBiaWg0S1NCN0NpQWdJQ0IyWVhJZ2FuTnZiazlpYWlBOUlFcFRUMDR1Y0dGeWMyVW9lQzVFWVhSaEtUc0tJQ0FnSUdwemIyNVBZbXBiSW1Gd2NGOXBaQ0pkSUQwZ0lqRWlPd29nSUNBZ2NtVjBkWEp1SUhzS0lDQWdJQ0FnSUNCRVlYUmhPaUJLVTA5T0xuTjBjbWx1WjJsbWVTaHFjMjl1VDJKcUtRb2dJQ0FnZlRzS2ZRPT0iCiAgfQp9"

# target
export TARGET_NAME="pubsub"                 \
TARGET_PUBSUB_PROJECT_ID="acme-project"     \
TARGET_PUBSUB_TOPIC_NAME="some-acme-topic"

# logging
export LOG_LEVEL="debug"

# reporting and stats
export SENTRY_DSN="https://acme.com/1"    \
SENTRY_DEBUG=true                         \
SENTRY_TAGS="{\"aKey\":\"aValue\"}"

export STATS_RECEIVER_NAME="statsd"                       \
STATS_RECEIVER_STATSD_ADDRESS="127.0.0.1:8125"            \
STATS_RECEIVER_STATSD_PREFIX="snowplow.stream-replicator" \
STATS_RECEIVER_TIMEOUT_SEC=2                              \
STATS_RECEIVER_BUFFER_SEC=20

export DISABLE_TELEMETRY=false         \
USER_PROVIDED_ID="elmer.fudd@acme.com"