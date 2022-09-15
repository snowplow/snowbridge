// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

// TODO: We might be able to factor the tests better and avoid some duplication here. Nice to have at this point.

func TestConfigurationOverview(t *testing.T) {
	assert := assert.New(t)
	// Read file:
	markdownFilePath := filepath.Join("documentation", "configuration", "configuration-overview.md")

	fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

	// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
	assert.Equal(1, len(fencedBlocksFound))
	// TODO: This won't give a very informative error. Fix that.

	// Test that source compiles
	testMinimalSourceConfig(t, fencedBlocksFound[0])

	// Thest that target compiles
	testMinimalTargetConfig(t, fencedBlocksFound[0])

	// Test that failure target compiles
	testFullFailureTargetConfig(t, fencedBlocksFound[0])

	// Test that transformations compile
	c := createConfigFromCodeBlock(t, fencedBlocksFound[0])

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
		fmt.Println(err.Error())
	}

	assert.Nil(err)
	assert.NotNil(statsd)

	// Test that sentry compiles
	// repeating some stuff here which can probably be factored better later.

	tmpConfigPath := filepath.Join("tmp", "config.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", tmpConfigPath)

	configFile, err := os.Create(tmpConfigPath)
	if err != nil {
		panic(err)
	}

	defer configFile.Close()

	// Write shortest one to minimal
	_, err2 := configFile.WriteString(fencedBlocksFound[0])
	if err != nil {
		assert.Fail(err.Error())
		panic(err2)
	}
	// Since sentry lives in cmd, we call Init to test it.
	cfgSentry, sentryEnabled, initErr := cmd.Init()
	fmt.Println(cfgSentry.Data.Sentry)

	assert.NotNil(cfgSentry)
	assert.True(sentryEnabled)
	assert.Nil(initErr)

}
