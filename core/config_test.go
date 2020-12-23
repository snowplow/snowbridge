// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c := NewConfig()
	assert.NotNil(c)

	assert.Equal("info", c.LogLevel)
	assert.Equal("stdout", c.Target)
	assert.Equal("stdin", c.Source)

	source, err := c.GetSource()
	assert.NotNil(source)
	assert.Nil(err)

	target, err := c.GetTarget()
	assert.NotNil(target)
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

	c := NewConfig()
	assert.NotNil(c)

	assert.Equal("debug", c.LogLevel)
	assert.Equal("kinesis", c.Target)
	assert.Equal("kinesis", c.Source)
}

func TestNewConfig_BooleanValues(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DEBUG")

	os.Setenv("SENTRY_DEBUG", "fake")

	c := NewConfig()
	assert.NotNil(c)
	assert.Equal(false, c.Sentry.Debug)

	os.Setenv("SENTRY_DEBUG", "true")

	c1 := NewConfig()
	assert.NotNil(c1)
	assert.Equal(true, c1.Sentry.Debug)
}

func TestNewConfig_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "fake")

	c := NewConfig()
	assert.NotNil(c)

	source, err := c.GetSource()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid source found; expected one of 'stdin, kinesis, pubsub' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidTarget(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("TARGET")

	os.Setenv("TARGET", "fake")

	c := NewConfig()
	assert.NotNil(c)

	source, err := c.GetTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub' and got 'fake'", err.Error())
}
