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

func TestInit_Success(t *testing.T) {
	assert := assert.New(t)

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("STATS_RECEIVER_TIMEOUT_SEC")

	os.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
}

func TestInit_Success_Sentry(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")
	defer os.Unsetenv("SENTRY_TAGS")

	os.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	os.Setenv("SENTRY_TAGS", "{\"client_name\":\"com.acme\"}")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure_LogLevel(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("LOG_LEVEL")

	os.Setenv("LOG_LEVEL", "DEBUG")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("Supported log levels are 'debug, info, warning, error, fatal, panic'; provided DEBUG", err.Error())
}

func TestInit_Failure_SentryDSN(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")

	os.Setenv("SENTRY_DSN", "blahblah")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("Failed to build Sentry: [Sentry] DsnParseError: invalid scheme", err.Error())
}

func TestInit_Failure_SentryTags(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("SENTRY_DSN")
	defer os.Unsetenv("SENTRY_TAGS")

	os.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	os.Setenv("SENTRY_TAGS", "asdasdasd")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)

	assert.Equal("Failed to unmarshall SENTRY_TAGS to map: invalid character 'a' looking for beginning of value", err.Error())
}
