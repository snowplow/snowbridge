// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

// TODO: We might be able to factor the tests better and avoid some duplication here. Nice to have at this point.

// TODO: When we provide the examples as full hcl files, we can factor all of the tests better & remove code.

func TestConfigurationOverview(t *testing.T) {
	assert := assert.New(t)

	hclFilePath := filepath.Join("documentation-examples", "configuration", "overview-full-example.hcl")

	// Test that source compiles
	testMinimalSourceConfig(t, hclFilePath)

	// Thest that target compiles
	testMinimalTargetConfig(t, hclFilePath)

	// Test that failure target compiles
	testFullFailureTargetConfig(t, hclFilePath)

	// Test that transformations compile
	c := getConfigFromFilepath(t, hclFilePath)

	transformFunc, err := transformconfig.GetTransformations(c)

	// For now, we're just testing that the config is valid here
	assert.NotNil(transformFunc)
	assert.Nil(err)

	// Test that statsd compiles
	statsdUse := c.Data.StatsReceiver.Receiver
	decoderOptsStatsd := &config.DecoderOptions{
		Input: statsdUse.Body,
	}

	statsd, err := c.CreateComponent(MockAdaptStatsDStatsReceiverNoDefaults(), decoderOptsStatsd)
	if err != nil {
		assert.Fail(err.Error())
	}

	assert.Nil(err)
	assert.NotNil(statsd)

	// Test that sentry compiles
	// repeating some stuff here which can probably be factored better later.

	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilePath)

	// Since sentry lives in cmd, we call Init to test it.
	cfgSentry, sentryEnabled, initErr := cmd.Init()

	assert.NotNil(cfgSentry)
	assert.True(sentryEnabled)
	assert.Nil(initErr)

}
