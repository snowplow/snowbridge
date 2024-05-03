//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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

func TestHandleSLULAEnvVar(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "true")
	assert.True(handleSLULAEnvVar())

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")
	assert.True(handleSLULAEnvVar())

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "1")
	assert.True(handleSLULAEnvVar())

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "on")
	assert.True(handleSLULAEnvVar())
}

func TestInit_Success(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_SLULAFailre(t *testing.T) {
	assert := assert.New(t)

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("please accept the terms of the Snowplow Limited Use License Agreement to proceed. See https://docs.snowplow.io/docs/destinations/forwarding-events/snowbridge/configuration/#license for more information on the license and how to configure this", err.Error())
	}
}

func TestInit_Failure(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "on")
	t.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to build config: Error parsing env config: env: parse error on field \"TimeoutSec\" of type \"int\": strconv.ParseInt: parsing \"debug\": invalid syntax", err.Error())
	}
}

func TestInit_Success_Sentry(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "1")
	t.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	t.Setenv("SENTRY_TAGS", "{\"client_name\":\"com.acme\"}")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure_LogLevel(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "true")
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

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")
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

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")
	t.Setenv("SENTRY_DSN", "https://1111111111111111111111111111111d@sentry.snplow.net/28")
	t.Setenv("SENTRY_TAGS", "asdasdasd")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to unmarshall SENTRY_TAGS to map: invalid character 'a' looking for beginning of value", err.Error())
	}
}
