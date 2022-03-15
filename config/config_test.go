// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The GetSource part needs to move anyway - causes circular dep.
func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal("info", c.Data.LogLevel)
	assert.Equal("stdout", c.Data.Target.Use.Name)
	assert.Equal("none", c.Data.Transformation)
	assert.Equal("stdin", c.Data.Source.Use.Name)

	// Tests on sources moved to the source package.

	target, err := c.GetTarget()
	assert.NotNil(target)
	assert.Nil(err)

	transformation, err := c.GetTransformations()
	assert.NotNil(transformation)
	assert.Nil(err)

	failureTarget, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.NotNil(failureTarget)
	assert.Nil(err)

	observer, err := c.GetObserver(map[string]string{})
	assert.NotNil(observer)
	assert.Nil(err)
}

func TestNewConfig_FromEnv(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("LOG_LEVEL")
	defer os.Unsetenv("TARGET_NAME")
	defer os.Unsetenv("SOURCE_NAME")

	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("TARGET_NAME", "kinesis")
	os.Setenv("SOURCE_NAME", "kinesis")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal("debug", c.Data.LogLevel)
	assert.Equal("kinesis", c.Data.Target.Use.Name)
	assert.Equal("kinesis", c.Data.Source.Use.Name)
}

func TestNewConfig_FromEnvInvalid(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("STATS_RECEIVER_TIMEOUT_SEC")

	os.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
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

	defer os.Unsetenv("TARGET_NAME")

	os.Setenv("TARGET_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureTarget(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGET_NAME")

	os.Setenv("FAILURE_TARGET_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureFormat(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("FAILURE_TARGETS_FORMAT")

	os.Setenv("FAILURE_TARGETS_FORMAT", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	source, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure format found; expected one of 'snowplow' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidStatsReceiver(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("STATS_RECEIVER_NAME")

	os.Setenv("STATS_RECEIVER_NAME", "fake")

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

	tags, err := c.GetTags()
	assert.NotNil(tags)
	assert.Nil(err)

	processID, ok := tags["process_id"]
	assert.NotEqual("", processID)
	assert.True(ok)
	hostname, ok := tags["host"]
	assert.NotEqual("", hostname)
	assert.True(ok)
}

func TestNewConfig_Hcl_invalids(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join("test-fixtures", "invalids.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	t.Run("invalid_transformation", func(t *testing.T) {
		transformation, err := c.GetTransformations()
		assert.Nil(transformation)
		assert.NotNil(err)
		assert.Equal("Invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk:{option}', spEnrichedFilter:{option} and got 'fakeHCL'", err.Error())
	})

	t.Run("invalid_target", func(t *testing.T) {
		target, err := c.GetTarget()
		assert.Nil(target)
		assert.NotNil(err)
		assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
	})

	t.Run("invalid_failure_target", func(t *testing.T) {
		ftarget, err := c.GetFailureTarget("testAppName", "0.0.0")
		assert.Nil(ftarget)
		assert.NotNil(err)
		assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
	})

}

func TestNewConfig_Hcl_defaults(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join("test-fixtures", "empty.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal(c.Data.Source.Use.Name, "stdin")
	assert.Equal(c.Data.Target.Use.Name, "stdout")
	assert.Equal(c.Data.FailureTarget.Target.Name, "stdout")
	assert.Equal(c.Data.FailureTarget.Format, "snowplow")
	assert.Equal(c.Data.Sentry.Tags, "{}")
	assert.Equal(c.Data.Sentry.Debug, false)
	assert.Equal(c.Data.StatsReceiver.TimeoutSec, 1)
	assert.Equal(c.Data.StatsReceiver.BufferSec, 15)
	assert.Equal(c.Data.Transformation, "none")
	assert.Equal(c.Data.LogLevel, "info")
}

func TestNewConfig_Hcl_sentry(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join("test-fixtures", "sentry.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal(c.Data.Sentry.Debug, true)
	assert.Equal(c.Data.Sentry.Tags, "{\"testKey\":\"testValue\"}")
	assert.Equal(c.Data.Sentry.Dsn, "testDsn")
}
