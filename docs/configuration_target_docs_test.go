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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/target"
	"github.com/stretchr/testify/assert"
)

func TestTargetDocumentation(t *testing.T) {

	// Set env vars referenced in the config examples
	t.Setenv("MY_AUTH_PASSWORD", "test")
	t.Setenv("SASL_PASSWORD", "test")
	t.Setenv("CLIENT_ID", "client_id_test")
	t.Setenv("CLIENT_SECRET", "client_secret_test")
	t.Setenv("REFRESH_TOKEN", "refresh_token_test")

	targetsToTest := []string{"eventhub", "http", "kafka", "kinesis", "pubsub", "sqs", "stdout"}

	for _, tgt := range targetsToTest {

		// Read file:
		minimalFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "targets", tgt+"-minimal-example.hcl")
		fullFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "targets", tgt+"-full-example.hcl")

		// Test minimal config
		testTargetConfig(t, minimalFilePath, false)

		// Test full config
		testTargetConfig(t, fullFilePath, true)
	}
}

func testTargetConfig(t *testing.T, filepath string, fullExample bool) {

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.Target.Use
	testTargetComponent(t, use.Name, use.Body, fullExample)
}

func testFailureTargetConfig(t *testing.T, filepath string, fullExample bool) {

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.FailureTarget.Target
	testTargetComponent(t, use.Name, use.Body, fullExample)
}

func testFilterTargetConfig(t *testing.T, filepath string, fullExample bool) {

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.FilterTarget.Use
	testTargetComponent(t, use.Name, use.Body, fullExample)
}

func testTargetComponent(t *testing.T, name string, body hcl.Body, fullExample bool) {
	assert := assert.New(t)
	var configObject interface{}
	switch name {
	case "eventhub":
		configObject = &target.EventHubConfig{}
	case "http":
		configObject = &target.HTTPTargetConfig{}
	case "kafka":
		configObject = &target.KafkaConfig{}
	case "kinesis":
		configObject = &target.KinesisTargetConfig{}
	case "pubsub":
		configObject = &target.PubSubTargetConfig{}
	case "sqs":
		configObject = &target.SQSTargetConfig{}
	case "stdout":
		// stdout doesn't have a config object, so we use an empty struct.
		var s struct{}
		configObject = &s
	default:
		assert.Fail(fmt.Sprint("Target not recognised: ", name))
	}

	// DecodeBody parses a hcl Body object into the provided struct.
	// It will fail if the configurations don't match, or if a required argument is missing.
	err := gohcl.DecodeBody(body, config.CreateHclContext(), configObject)
	if err != nil {
		assert.Fail(name, err.Error())
	}

	if fullExample {
		checkComponentForZeros(t, configObject)
	}

}
