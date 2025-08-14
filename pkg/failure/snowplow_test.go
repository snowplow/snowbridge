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

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

// --- Test FailureTarget

type TestFailureTarget struct {
	onWrite func(messages []*models.Message) (*models.TargetWriteResult, error)
}

func (t *TestFailureTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	return t.onWrite(messages)
}

func (t *TestFailureTarget) Open() {}

func (t *TestFailureTarget) Close() {}

func (t *TestFailureTarget) MaximumAllowedMessageSizeBytes() int {
	return 5000
}

func (t *TestFailureTarget) GetID() string {
	return "empty"
}

// --- Tests

func TestSnowplowFailure_WriteOversized(t *testing.T) {
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

	onWrite := func(messages []*models.Message) (*models.TargetWriteResult, error) {
		assert.Equal(5, len(messages))
		for _, msg := range messages {
			assert.Equal(string(expectedJSONString), string(msg.Data))
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewSnowplowFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)
	assert.Equal("empty", sf.GetID())

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello Snowplow!!", nil)

	r, err := sf.WriteOversized(5000, messages)
	assert.Nil(r)
	assert.Nil(err)
}

func TestSnowplowFailure_WriteInvalid(t *testing.T) {
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

	onWrite := func(messages []*models.Message) (*models.TargetWriteResult, error) {
		assert.Equal(5, len(messages))
		for _, msg := range messages {
			assert.Equal(string(expectedJSONString), string(msg.Data))
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewSnowplowFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello Snowplow!!", nil)
	for _, msg := range messages {
		msg.SetError(errors.New("failure"))
	}

	r, err := sf.WriteInvalid(messages)
	assert.Nil(r)
	assert.Nil(err)
}
