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

func TestInit_Success(t *testing.T) {
	assert := assert.New(t)

	cfg, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Success_Sentry(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")
	defer os.Unsetenv("SENTRY_TAGS")

	os.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	os.Setenv("SENTRY_TAGS", "{\"client_name\":\"com.acme\"}")

	cfg, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure_LogLevel(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("LOG_LEVEL")

	os.Setenv("LOG_LEVEL", "DEBUG")

	cfg, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("FATAL: Supported log levels are 'debug, info, warning, error, fatal, panic' - provided: DEBUG", err.Error())
}

func TestInit_Failure_SentryDSN(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")

	os.Setenv("SENTRY_DSN", "blahblah")

	cfg, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("FATAL: sentry.Init: [Sentry] DsnParseError: invalid scheme", err.Error())
}

func TestInit_Failure_SentryTags(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")
	defer os.Unsetenv("SENTRY_TAGS")

	os.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	os.Setenv("SENTRY_TAGS", "asdasdasd")

	cfg, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("FATAL: Failed to unmarshall SENTRY_TAGS to map: invalid character 'a' looking for beginning of value", err.Error())
}
