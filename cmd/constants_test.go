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

package cmd

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModulePathMatchesAppVersion(t *testing.T) {
	assert := assert.New(t)

	goMod, err := os.ReadFile("../go.mod")
	require.NoError(t, err)

	var modulePath string
	for _, line := range strings.Split(string(goMod), "\n") {
		if strings.HasPrefix(line, "module ") {
			modulePath = strings.TrimPrefix(line, "module ")
			break
		}
	}
	require.NotEmpty(t, modulePath, "could not find module path in go.mod")

	// Extract major version from module path (e.g. "github.com/snowplow/snowbridge/v5" -> "5")
	parts := strings.Split(modulePath, "/")
	moduleVersion := parts[len(parts)-1]
	require.True(t, strings.HasPrefix(moduleVersion, "v"), "module path should end with a version segment like /v5, got: %s", moduleVersion)
	moduleMajor := strings.TrimPrefix(moduleVersion, "v")

	// Extract major version from AppVersion (e.g. "5.1.0" -> "5")
	appMajor := strings.Split(AppVersion, ".")[0]

	assert.Equal(appMajor, moduleMajor, "go.mod module path major version (%s) does not match AppVersion major version (%s)", moduleMajor, appMajor)
}
