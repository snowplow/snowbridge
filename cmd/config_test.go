// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal("info", c.LogLevel)
	assert.Equal("stdout", c.Target)
	assert.Equal("none", c.Transformation)
	assert.Equal("stdin", c.Source)

	source, err := c.GetSource()
	assert.NotNil(source)
	assert.Nil(err)

	target, err := c.GetTarget()
	assert.NotNil(target)
	assert.Nil(err)

	transformation, err := c.GetTransformations()
	assert.NotNil(transformation)
	assert.Nil(err)

	failureTarget, err := c.GetFailureTarget()
	assert.NotNil(failureTarget)
	assert.Nil(err)

	observer, err := c.GetObserver(map[string]string{})
	assert.NotNil(observer)
	assert.Nil(err)
}

func TestNewConfig_FromEnv(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("LOG_LEVEL")
	defer os.Unsetenv("TARGET")
	defer os.Unsetenv("SOURCE")

	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("TARGET", "kinesis")
	os.Setenv("SOURCE", "kinesis")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal("debug", c.LogLevel)
	assert.Equal("kinesis", c.Target)
	assert.Equal("kinesis", c.Source)
}

func TestNewConfig_FromEnvInvalid(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("STATS_RECEIVER_TIMEOUT_SEC")

	os.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
}

func TestNewConfig_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetSource()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid source found; expected one of 'stdin, kinesis, pubsub, sqs' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidTransformation(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("MESSAGE_TRANSFORMATION")

	os.Setenv("MESSAGE_TRANSFORMATION", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	transformation, err := c.GetTransformations()
	assert.Nil(transformation)
	assert.NotNil(err)
	assert.Equal("Invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk:{option}', spEnrichedFilter:{option} and got 'fake'", err.Error())
}

func TestNewConfig_FilterFailure(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("MESSAGE_TRANSFORMATION")

	os.Setenv("MESSAGE_TRANSFORMATION", "spEnrichedFilter:incompatibleArg")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	transformation, err := c.GetTransformations()
	assert.Nil(transformation)
	assert.NotNil(err)
	assert.Equal(`Invalid filter function config, must be of the format {field name}=={value}[|{value}|...] or {field name}!={value}[|{value}|...]`, err.Error())
}

func TestNewConfig_InvalidTarget(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("TARGET")

	os.Setenv("TARGET", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureTarget(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGET")

	os.Setenv("FAILURE_TARGET", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetFailureTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureFormat(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGETS_FORMAT")

	os.Setenv("FAILURE_TARGETS_FORMAT", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetFailureTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure format found; expected one of 'snowplow' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidStatsReceiver(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("STATS_RECEIVER")

	os.Setenv("STATS_RECEIVER", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetObserver(map[string]string{})
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid stats receiver found; expected one of 'statsd' and got 'fake'", err.Error())
}

func TestNewConfig_GetTags(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	tags, err := c.GetTags("source", "target", "failure_target")
	assert.NotNil(tags)
	assert.Nil(err)

	processID, ok := tags["process_id"]
	assert.NotEqual("", processID)
	assert.True(ok)
	hostname, ok := tags["hostname"]
	assert.NotEqual("", hostname)
	assert.True(ok)
	source, ok := tags["source_id"]
	assert.Equal("source", source)
	assert.True(ok)
	target, ok := tags["target_id"]
	assert.Equal("target", target)
	assert.True(ok)
	failureTarget, ok := tags["failure_target_id"]
	assert.Equal("failure_target", failureTarget)
	assert.True(ok)
}

func TestNewConfig_KafkaTargetDefaults(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("TARGET")

	os.Setenv("TARGET", "kafka")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	target := c.Targets.Kafka
	assert.NotNil(target)
	assert.Equal(target.MaxRetries, 10)
	assert.Equal(target.ByteLimit, 1048576)
	assert.Equal(target.Compress, false)
	assert.Equal(target.WaitForAll, false)
	assert.Equal(target.Idempotent, false)
	assert.Equal(target.EnableSASL, false)
	assert.Equal(target.ForceSyncProducer, false)
	assert.Equal(target.FlushFrequency, 0)
	assert.Equal(target.FlushMessages, 0)
	assert.Equal(target.FlushBytes, 0)
}

func TestNewConfig_KafkaFailureTargetDefaults(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGET")

	os.Setenv("FAILURE_TARGET", "kafka")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	target := c.FailureTargets.Kafka
	assert.NotNil(target)
	assert.Equal(target.MaxRetries, 10)
	assert.Equal(target.ByteLimit, 1048576)
	assert.Equal(target.Compress, false)
	assert.Equal(target.WaitForAll, false)
	assert.Equal(target.Idempotent, false)
	assert.Equal(target.EnableSASL, false)
	assert.Equal(target.ForceSyncProducer, false)
	assert.Equal(target.FlushFrequency, 0)
	assert.Equal(target.FlushMessages, 0)
	assert.Equal(target.FlushBytes, 0)
}

func TestNewConfig_EventhubTargetDefaults(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("TARGET")
	defer os.Unsetenv("TARGET_EVENTHUB_NAMESPACE")
	defer os.Unsetenv("TARGET_EVENTHUB_NAME")

	os.Setenv("TARGET", "eventhub")
	os.Setenv("TARGET_EVENTHUB_NAMESPACE", "fake")
	os.Setenv("TARGET_EVENTHUB_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	target := c.Targets.EventHub
	assert.NotNil(target)
	assert.Equal(target.EventHubName, "fake")
	assert.Equal(target.EventHubNamespace, "fake")
	assert.Equal(target.MessageByteLimit, 1048576)
	assert.Equal(target.ChunkByteLimit, 1048576)
	assert.Equal(target.ChunkMessageLimit, 500)
	assert.Equal(target.ContextTimeoutInSeconds, 20)
	assert.Equal(target.Batching, true)
	assert.Equal(target.BatchByteLimit, 1048576)
}

func TestNewConfig_EventhubFailureTargetDefaults(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGET")
	defer os.Unsetenv("FAILURE_TARGET_EVENTHUB_NAMESPACE")
	defer os.Unsetenv("FAILURE_TARGET_EVENTHUB_NAME")

	os.Setenv("FAILURE_TARGET", "eventhub")
	os.Setenv("FAILURE_TARGET_EVENTHUB_NAMESPACE", "fake")
	os.Setenv("FAILURE_TARGET_EVENTHUB_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	target := c.FailureTargets.EventHub
	assert.NotNil(target)
	assert.Equal(target.EventHubName, "fake")
	assert.Equal(target.EventHubNamespace, "fake")
	assert.Equal(target.MessageByteLimit, 1048576)
	assert.Equal(target.ChunkByteLimit, 1048576)
	assert.Equal(target.ChunkMessageLimit, 500)
	assert.Equal(target.ContextTimeoutInSeconds, 20)
	assert.Equal(target.Batching, true)
	assert.Equal(target.BatchByteLimit, 1048576)
}
