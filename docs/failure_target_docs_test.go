// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/stretchr/testify/assert"
)

// Since failure targets are just targets under the hood, their configurations are identical at present, and the docs reflects that.
// So we don't need tests for failuret targets until that changes. The below, hoewever, is used in the configuration overview test - so we keep it.
func testFullFailureTargetConfig(t *testing.T, filepath string) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.FailureTarget.Target
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

	// TODO: We can replace this with checkComponentForZeros.

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
