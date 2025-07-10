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

	"github.com/snowplow/snowbridge/assets"
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

	// Test that sentry compiles
	testSentryConfig(t, hclFilePath, false)

	// Test that webhook monitoring (heartbeat & alert) compiles
	testWebhookConfig(t, hclFilePath, false)
}
