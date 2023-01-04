//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
