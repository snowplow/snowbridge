// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/cmd"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/statsreceiver"
	"github.com/stretchr/testify/assert"
)

func TestMonitoringDocumentation(t *testing.T) {
	assert := assert.New(t)

	statsDFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "monitoring", "statsd-example.hcl")

	testStatsDConfig(t, statsDFilePath, true)

	loglevelFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "monitoring", "log-level-example.hcl")

	loglevelConf := getConfigFromFilepath(t, loglevelFilePath)

	// Check that loglevel isn't the default.
	assert.NotEqual("info", loglevelConf.Data.LogLevel)

	// Repeating some code from  createConfigFromCodeBlock here so we can call init.
	// We can prob factor better.

	sentryFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "monitoring", "sentry-example.hcl")

	testSentryConfig(t, sentryFilePath, true)

}

func testStatsDConfig(t *testing.T, configpath string, fullExample bool) {
	assert := assert.New(t)
	c := getConfigFromFilepath(t, configpath)

	confStatsRec := c.Data.StatsReceiver

	configObject := &statsreceiver.StatsDStatsReceiverConfig{}

	err := gohcl.DecodeBody(confStatsRec.Receiver.Body, config.CreateHclContext(), configObject)
	if err != nil {
		assert.Fail(confStatsRec.Receiver.Name, err.Error())
	}

	if fullExample {
		checkComponentForZeros(t, configObject)

		// Check the config values that are outside the statsreceiver part
		assert.NotZero(confStatsRec.BufferSec)
		assert.NotZero(confStatsRec.TimeoutSec)
	}
}

func testSentryConfig(t *testing.T, configpath string, fullExample bool) {
	assert := assert.New(t)
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", configpath)

	// Since sentry lives in cmd, we call Init to test it.
	cfgSentry, sentryEnabled, initErr := cmd.Init()

	assert.NotNil(cfgSentry)
	assert.True(sentryEnabled)
	assert.Nil(initErr)

	if fullExample {
		checkComponentForZeros(t, cfgSentry.Data.Sentry)
	}

}
