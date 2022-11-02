// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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

	// Test that transformations compile
	testTransformationConfig(t, hclFilePath, false)

	// Test that statsd compiles
	testStatsDConfig(t, hclFilePath, false)

	// Test that sentry compiles
	testSentryConfig(t, hclFilePath, false)

}
