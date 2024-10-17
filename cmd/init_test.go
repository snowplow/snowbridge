/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
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

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_SLULAFailure(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("please accept the terms of the Snowplow Limited Use License Agreement to proceed. See https://docs.snowplow.io/docs/destinations/forwarding-events/snowbridge/configuration/#license for more information on the license and how to configure this", err.Error())
	}
}

func TestInit_NewConfigFailure(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "fail.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "on")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal(strings.Contains(err.Error(), "Failed to build config"), true)
	}
}

func TestInit_Success_Sentry(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "sentry-valid.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "1")

	cfg, _, err := Init()
	assert.NotNil(cfg)
	assert.Nil(err)
}

func TestInit_Failure_LogLevel(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "invalids.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

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

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "sentry.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to build Sentry: [Sentry] DsnParseError: invalid scheme", err.Error())
	}
}

func TestInit_Failure_SentryTags(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "sentry-invalid-tags.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	t.Setenv("ACCEPT_LIMITED_USE_LICENSE", "yes")

	cfg, _, err := Init()
	assert.Nil(cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to unmarshall SENTRY_TAGS to map: invalid character 'a' looking for beginning of value", err.Error())
	}
}
