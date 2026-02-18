//go:build awsonly

// This version of source config imports kinsumer, and is only included in aws only builds.

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
	"fmt"
	"testing"

	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

// To run this test: `go test -tags=awsonly`
// Separate test for the aws only build is required to verify that the one behaviour difference works
func TestGetSource_WithKinesisSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	hclConfig := []byte(fmt.Sprintf(`
		source {
			use "kinesis" {
    			app_name    		= "test-app"
    			stream_name 		= "test-stream"
    			region      		= "%s"
				custom_aws_endpoint = "%s"
			}
		}
	`, testutil.AWSLocalstackRegion, testutil.AWSLocalstackEndpoint))

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	kinesisSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(kinesisSource)
}
