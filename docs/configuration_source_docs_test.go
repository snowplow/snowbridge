//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package docs

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	kinesissource "github.com/snowplow/snowbridge/pkg/source/kinesis"
	pubsubsource "github.com/snowplow/snowbridge/pkg/source/pubsub"
	sqssource "github.com/snowplow/snowbridge/pkg/source/sqs"
	stdinsource "github.com/snowplow/snowbridge/pkg/source/stdin"
	"github.com/stretchr/testify/assert"
)

func TestSourceDocumentation(t *testing.T) {
	// Set env vars referenced in the config examples
	t.Setenv("MY_AUTH_PASSWORD", "test")
	t.Setenv("SASL_PASSWORD", "test")

	sourcesToTest := []string{"kinesis", "pubsub", "sqs", "stdin"}

	for _, src := range sourcesToTest {

		// Read file:
		minimalFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "sources", src+"-minimal-example.hcl")
		fullFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "sources", src+"-full-example.hcl")

		// Test minimal config
		testSourceConfig(t, minimalFilePath, false)
		// Test full config
		testSourceConfig(t, fullFilePath, true)
	}
}

func testSourceConfig(t *testing.T, filepath string, fullExample bool) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.Source.Use

	var configObject interface{}
	switch use.Name {
	case "kinesis":
		configObject = &kinesissource.Configuration{}
	case "pubsub":
		configObject = &pubsubsource.Configuration{}
	case "sqs":
		configObject = &sqssource.Configuration{}
	case "stdin":
		configObject = &stdinsource.Configuration{}
	default:
		assert.Fail(fmt.Sprint("Source not recognised: ", use.Name))
	}

	// DecodeBody parses a hcl Body object into the provided struct.
	// It will fail if the configurations don't match, or if a required argument is missing.
	err := gohcl.DecodeBody(use.Body, config.CreateHclContext(), configObject)
	if err != nil {
		assert.Fail(use.Name, err.Error())
	}

	if fullExample {
		checkComponentForZeros(t, configObject)
	}
}
