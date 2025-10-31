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

func TestRetryConfigDocumentation(t *testing.T) {
	assert := assert.New(t)
	retryFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "retry-example.hcl")
	c := getConfigFromFilepath(t, retryFilePath)

	retryConfig := c.Data.Retry
	assert.NotNil(retryConfig)
	assert.Equal(5000, retryConfig.Transient.Delay)
	assert.Equal(10, retryConfig.Transient.MaxAttempts)
	assert.Equal(30000, retryConfig.Setup.Delay)
	assert.Equal(3, retryConfig.Setup.MaxAttempts)
}
