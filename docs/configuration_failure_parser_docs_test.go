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

	"github.com/snowplow/snowbridge/v3/assets"
	"github.com/stretchr/testify/assert"
)

func TestFailureParserConfigDocumentation(t *testing.T) {
	minimalFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "failure-parser-minimal-example.hcl")
	fullFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "failure-parser-full-example.hcl")

	testFailureParserConfig(t, minimalFilePath, false)

	testFailureParserConfig(t, fullFilePath, true)
}

func testFailureParserConfig(t *testing.T, filepath string, fullExample bool) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	failureParserConfig := c.Data.FailureParser
	assert.NotNil(failureParserConfig)

	if fullExample {
		checkComponentForZeros(t, failureParserConfig)
	}
}
