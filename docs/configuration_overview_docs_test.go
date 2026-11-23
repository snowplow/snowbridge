/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package docs

import (
	"path/filepath"
	"testing"

	"github.com/snowplow/snowbridge/v6/assets"
	"github.com/stretchr/testify/assert"
)

func TestConfigurationOverview(t *testing.T) {

	t.Setenv("JS_SCRIPT_PATH", jsScriptPath)

	hclFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "overview-full-example.hcl")

	// Test that source compiles
	testSourceConfig(t, hclFilePath, false)

	// Thest that target compiles
	testTargetConfig(t, hclFilePath, false)

	// Test that failure target compiles
	testFailureTargetConfig(t, hclFilePath, false)

	// Test that filter target compiles
	testFilterTargetConfig(t, hclFilePath, false)

	// Test that transformations compile
	testTransformationConfig(t, hclFilePath, false)

	// Test that statsd compiles
	testStatsDConfig(t, hclFilePath, false)

	// Test that failure parser compiles
	testFailureParserConfig(t, hclFilePath, false)

	// Test that sentry compiles
	testSentryConfig(t, hclFilePath, false)

	// Test that webhook monitoring (heartbeat & alert) compiles
	testWebhookConfig(t, hclFilePath, false)

	// Test that decompression compiles and covers all fields
	testDecompressionConfig(t, hclFilePath)

	// Test that forwarder compiles and covers all fields
	testForwarderConfig(t, hclFilePath)
}

func testDecompressionConfig(t *testing.T, configpath string) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, configpath)

	decompressionConfig := c.Data.Decompression

	assert.NotNil(decompressionConfig)
	assert.Equal(5000000, decompressionConfig.MaxBytesSinglePayload)

	checkComponentForZeros(t, decompressionConfig)
}

func testForwarderConfig(t *testing.T, configpath string) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, configpath)

	forwarderConfig := c.Data.Forwarder

	assert.NotNil(forwarderConfig)
	assert.Equal("7f3c9a2e-1b4d-4e8f-9c6a-2d5e8b1f0a3c", forwarderConfig.ID)
	assert.Equal("My forwarder", forwarderConfig.Name)

	checkComponentForZeros(t, forwarderConfig)
}
