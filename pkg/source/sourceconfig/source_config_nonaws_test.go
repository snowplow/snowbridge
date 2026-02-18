//go:build !awsonly

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

package sourceconfig

import (
	"testing"

	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/stretchr/testify/assert"
)

func TestGetSource_WithKinesisSource(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		source {
			use "kinesis" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	kinesisSource, _, err := GetSource(c, nil)

	assert.Error(err)
	assert.ErrorContains(err, "kinesis source is not supported in this build, use the aws-only build instead")
	assert.Nil(kinesisSource)
}
