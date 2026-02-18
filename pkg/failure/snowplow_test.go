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

package failure

import (
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

// --- Tests

func TestSnowplowFailure_MakeOversizedPayloads(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"failure": map[string]any{
				"actualSizeBytes":         16,
				"expectation":             "Expected payload to fit into requested target",
				"maximumAllowedSizeBytes": 5000,
				"timestamp":               "0001-01-01T00:00:00Z",
			},
			"payload": "Hello Snowplow!!",
			"processor": map[string]string{
				"artifact": "test",
				"version":  "0.1.0",
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	sf, err := NewSnowplowFailure(5000, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	messages := testutil.GetTestMessages(5, "Hello Snowplow!!", nil)

	result, err := sf.MakeOversizedPayloads(5000, messages)
	assert.Nil(err)
	assert.Equal(5, len(result))

	for _, msg := range result {
		assert.Equal(string(expectedJSONString), string(msg.Data))
	}
}

func TestSnowplowFailure_MakeInvalidPayloads(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"failure": map[string]any{
				"errors":    []string{"failure"},
				"timestamp": "0001-01-01T00:00:00Z",
			},
			"payload": "Hello Snowplow!!",
			"processor": map[string]string{
				"artifact": "test",
				"version":  "0.1.0",
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	sf, err := NewSnowplowFailure(5000, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	messages := testutil.GetTestMessages(5, "Hello Snowplow!!", nil)
	for _, msg := range messages {
		msg.SetError(errors.New("failure"))
	}

	result, err := sf.MakeInvalidPayloads(messages)
	assert.Nil(err)
	assert.Equal(5, len(result))

	for _, msg := range result {
		assert.Equal(string(expectedJSONString), string(msg.Data))
	}
}
