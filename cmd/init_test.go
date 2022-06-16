// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestInit_Success(t *testing.T) {
	assert := assert.New(t)

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to build config: env: parse error on field \"TimeoutSec\" of type \"int\": strconv.ParseInt: parsing \"debug\": invalid syntax", err.Error())
	}
}

func TestInit_Success_Sentry(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	t.Setenv("SENTRY_TAGS", "{\"client_name\":\"com.acme\"}")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure_LogLevel(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("LOG_LEVEL", "DEBUG")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Supported log levels are 'debug, info, warning, error, fatal, panic'; provided DEBUG", err.Error())
	}
}

func TestInit_Failure_SentryDSN(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SENTRY_DSN", "blahblah")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to build Sentry: [Sentry] DsnParseError: invalid scheme", err.Error())
	}
}

func TestInit_Failure_SentryTags(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	t.Setenv("SENTRY_TAGS", "asdasdasd")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to unmarshall SENTRY_TAGS to map: invalid character 'a' looking for beginning of value", err.Error())
	}
}
