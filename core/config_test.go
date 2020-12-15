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
}

func TestNewConfig_FromEnv(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("TARGET", "kinesis")

	defer os.Unsetenv("LOG_LEVEL")
	defer os.Unsetenv("TARGET")

	c := NewConfig()
	assert.NotNil(c)

	assert.Equal("debug", c.LogLevel)
	assert.Equal("kinesis", c.Target)
}
