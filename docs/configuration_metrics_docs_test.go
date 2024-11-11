/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package docs

import (
	"path/filepath"
	"testing"

	"github.com/snowplow/snowbridge/assets"
	"github.com/stretchr/testify/assert"
)

func TestMetricsConfigDocumentation(t *testing.T) {
	assert := assert.New(t)
	configPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "metrics", "e2e-latency-example.hcl")
	c := getConfigFromFilepath(t, configPath)

	metricsConfig := c.Data.Metrics
	assert.NotNil(metricsConfig)
	assert.Equal(true, metricsConfig.E2ELatencyEnabled)
}
