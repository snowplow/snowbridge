// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/stretchr/testify/assert"
)

func TestTargetDocumentation(t *testing.T) {
	assert := assert.New(t)

	// Set env vars referenced in the config examples
	t.Setenv("MY_AUTH_PASSWORD", "test")
	t.Setenv("SASL_PASSWORD", "test")

	targetsToTest := []string{"eventhub", "http", "kafka", "kinesis", "pubsub", "sqs"}

	for _, tgt := range targetsToTest {

		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "targets", tgt+".md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound), "Unexpected number of hcl blocks found")
		assert.LessOrEqual(len(fencedBlocksFound), 2, "Unexpected number of hcl blocks found")

		// Sort by length to determine which is the minimal example.
		sort.Slice(fencedBlocksFound, func(i, j int) bool {
			return len(fencedBlocksFound[i]) < len(fencedBlocksFound[j])
		})

		// Test minimal config
		// Shortest is always minimal
		testMinimalTargetConfig(t, fencedBlocksFound[0])
		// Test full config
		// Longest is the full config. Where there are no required arguments, there is only one config.
		// In that scenario, both tests should pass.
		testFullTargetConfig(t, fencedBlocksFound[len(fencedBlocksFound)-1])
	}
}

func testMinimalTargetConfig(t *testing.T, codeBlock string) {
	assert := assert.New(t)

	c := createConfigFromCodeBlock(t, codeBlock)

	use := c.Data.Target.Use
	decoderOpts := &config.DecoderOptions{
		Input: use.Body,
	}

	var result interface{}
	var err error

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
		result, err = nil, errors.New(fmt.Sprint("Target not recognised: ", use.Name))
	}

	assert.NotNil(result)
	if err != nil {
		assert.Fail(use.Name, err.Error())
	}
	assert.Nil(err)
}

func testFullTargetConfig(t *testing.T, codeBlock string) {
	assert := assert.New(t)

	c := createConfigFromCodeBlock(t, codeBlock)

	use := c.Data.Target.Use
	decoderOpts := &config.DecoderOptions{
		Input: use.Body,
	}

	var result interface{}
	var err error

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
	case "stdout":
		// No need to mock this one, there's no options in the config and it doesn't build a client.
		result, err = c.CreateComponent(target.AdaptStdoutTargetFunc(target.StdoutTargetConfigFunction), decoderOpts)
	default:
		result, err = nil, errors.New(fmt.Sprint("Target not recognised: ", use.Name))
	}

	if err != nil {
		assert.Fail(use.Name, err.Error())
	}
	assert.Nil(err)
	assert.NotNil(result)

	// Skip the next bit if we failed the above - it will panic if result doesn't exist, making the debug annoying
	if result == nil {
		return
	}

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
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", typeOfRslt, zerosFound))
}
