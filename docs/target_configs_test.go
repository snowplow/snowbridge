// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/stretchr/testify/assert"
)

// TODO: This passes when we remove a case from targetsToTest. Is there an approach which fails if we miss a case?

// TestTargetDocsFullConfigs tests config examples who have all options fully populated.
// We use mocks to prevent default values from being set.
// This test will fail if we either don't have an example config file for a target, or we do have one but not all values are set.
// Because we can't tell between a missing and a zero value, zero values in the examples will cause a false negative.
func TestTargetDocsFullConfigs(t *testing.T) {
	assert := assert.New(t)

	// Because we use `env` in examples, we need this to be set.
	t.Setenv("MY_AUTH_PASSWORD", "test")

	targetsToTest := []string{"eventhub", "http", "kafka", "kinesis", "pubsub", "sqs"}

	for _, trgt := range targetsToTest {
		hclFilename := filepath.Join("configs", "target", "full", fmt.Sprintf("%s-full.hcl", trgt))
		t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

		c, err := config.NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		use := c.Data.Target.Use
		decoderOpts := &config.DecoderOptions{
			Input: use.Body,
		}

		var result interface{}

		switch use.Name {
		case "eventhub":
			result, err = c.CreateComponent(EventHubTargetAdapterNoDefaults(), decoderOpts)
		case "http":
			result, err = c.CreateComponent(HTTPTargetAdapterNoDefaults(), decoderOpts)
		case "kafka":
			result, err = c.CreateComponent(KafkaTargetAdapterNoDefaults(), decoderOpts)
		case "kinesis":
			result, err = c.CreateComponent(KinesisTargetAdapterNoDefaults(), decoderOpts)
		case "pubsub":
			result, err = c.CreateComponent(PubSubTargetAdapterNoDefaults(), decoderOpts)
		case "sqs":
			result, err = c.CreateComponent(SQSTargetAdapterNoDefaults(), decoderOpts)
		default:
			assert.Fail(fmt.Sprintf("Target not recognised: %v", trgt))
		}

		assert.Nil(err)
		assert.NotNil(result)

		// Indirect dereferences the pointer for us
		valOfRslt := reflect.Indirect(reflect.ValueOf(result))
		typeOfRslt := valOfRslt.Type()

		var zerosFound []string

		for i := 0; i < typeOfRslt.NumField(); i++ {
			if valOfRslt.Field(i).IsZero() {
				zerosFound = append(zerosFound, typeOfRslt.Field(i).Name)
			}
		}

		// Check for empty fields in example config
		assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config %v - for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", hclFilename, typeOfRslt, zerosFound))
	}
}

// TestTargetDocsMinimalConfigs tests minimal config examples.
func TestTargetDocsMinimalConfigs(t *testing.T) {
	assert := assert.New(t)

	targetsToTest := []string{"eventhub", "http", "kafka", "kinesis", "pubsub", "sqs", "stdout"}

	for _, trgt := range targetsToTest {
		hclFilename := filepath.Join("configs", "target", "minimal", fmt.Sprintf("%s-minimal.hcl", trgt))
		t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

		c, err := config.NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		use := c.Data.Target.Use
		decoderOpts := &config.DecoderOptions{
			Input: use.Body,
		}

		var result interface{}

		switch use.Name {
		case "eventhub":
			result, err = c.CreateComponent(config.MockEventHubTargetAdapter(), decoderOpts)
		case "http":
			result, err = c.CreateComponent(config.MockHTTPTargetAdapter(), decoderOpts)
		case "kafka":
			result, err = c.CreateComponent(config.MockKafkaTargetAdapter(), decoderOpts)
		case "kinesis":
			result, err = c.CreateComponent(config.MockKinesisTargetAdapter(), decoderOpts)
		case "pubsub":
			result, err = c.CreateComponent(config.MockPubSubTargetAdapter(), decoderOpts)
		case "sqs":
			result, err = c.CreateComponent(config.MockSQSTargetAdapter(), decoderOpts)
		case "stdout":
			// No need to mock this one, there's no options in the config and it doesn't build a client.
			result, err = c.CreateComponent(target.AdaptStdoutTargetFunc(target.StdoutTargetConfigFunction), decoderOpts)
		default:
			assert.Fail(fmt.Sprintf("Target not recognised: %v", trgt))
		}
		assert.Nil(err)
		assert.NotNil(result)

		// No checks for zero values needed here.

		// TODO: Think about any way to verify if the provided config is genuinely minimum required config...
		// If we set a value for which there's a default, currently it will still pass
	}
}
