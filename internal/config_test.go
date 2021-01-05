// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package internal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	assert.Equal("info", c.LogLevel)
	assert.Equal("stdout", c.Target)
	assert.Equal("stdin", c.Source)

	source, err := c.GetSource()
	assert.NotNil(source)
	assert.Nil(err)

	target, err := c.GetTarget()
	assert.NotNil(target)
	assert.Nil(err)

	failureTarget, err := c.GetFailureTarget()
	assert.NotNil(failureTarget)
	assert.Nil(err)

	observer, err := c.GetObserver()
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
	assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs' and got 'fake'", err.Error())
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
	assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs' and got 'fake'", err.Error())
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

	source, err := c.GetObserver()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid stats receiver found; expected one of 'statsd' and got 'fake'", err.Error())
}
