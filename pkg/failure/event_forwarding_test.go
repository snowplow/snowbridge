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

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestEventForwardingFailure_WriteOversized(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"failure": map[string]any{
				"actualSizeBytes":         23,
				"expectation":             "Expected payload to fit into requested target",
				"maximumAllowedSizeBytes": 5000,
				"timestamp":               "0001-01-01T00:00:00Z",
			},
			"payload": "Hello EventForwarding!!",
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
			diff, err := testutil.GetJsonDiff(string(expectedJSONString), string(msg.Data))
			assert.Nil(err)
			assert.Zero(diff)
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewEventForwardingFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)
	assert.Equal("empty", sf.GetID())

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello EventForwarding!!", nil)

	r, err := sf.WriteOversized(5000, messages)
	assert.Nil(r)
	assert.Nil(err)
}

func TestEventForwardingFailure_WriteInvalidTransformationError(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"processor": map[string]string{
				"artifact": "test",
				"version":  "0.1.0",
			},
			"payload": "EventForwarding",
			"failure": map[string]string{
				"latestState":  "Hello EventForwarding!!",
				"timestamp":    "0001-01-01T00:00:00Z",
				"errorType":    "transformation",
				"errorMessage": "failure: failure",
				"errorCode":    "GenericError",
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	onWrite := func(messages []*models.Message) (*models.TargetWriteResult, error) {
		assert.Equal(5, len(messages))
		for _, msg := range messages {
			diff, err := testutil.GetJsonDiff(string(expectedJSONString), string(msg.Data))
			assert.Nil(err)
			assert.Zero(diff)
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewEventForwardingFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello EventForwarding!!", nil)
	for _, msg := range messages {
		msg.OriginalData = []byte("EventForwarding")
		msg.SetError(&models.TransformationError{
			SafeMessage: "failure",
			Err:         errors.New("failure"),
		})
	}

	r, err := sf.WriteInvalid(messages)
	assert.Nil(r)
	assert.Nil(err)
}

func TestEventForwardingFailure_WriteInvalidTemplatingError(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"processor": map[string]string{
				"artifact": "test",
				"version":  "0.1.0",
			},
			"payload": "EventForwarding",
			"failure": map[string]string{
				"latestState":  "Hello EventForwarding!!",
				"timestamp":    "0001-01-01T00:00:00Z",
				"errorType":    "template",
				"errorMessage": "failure: failure",
				"errorCode":    "",
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	onWrite := func(messages []*models.Message) (*models.TargetWriteResult, error) {
		assert.Equal(5, len(messages))
		for _, msg := range messages {
			diff, err := testutil.GetJsonDiff(string(expectedJSONString), string(msg.Data))
			assert.Nil(err)
			assert.Zero(diff)
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewEventForwardingFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello EventForwarding!!", nil)
	for _, msg := range messages {
		msg.OriginalData = []byte("EventForwarding")
		msg.SetError(&models.TemplatingError{
			SafeMessage: "failure",
			Err:         errors.New("failure"),
		})
	}

	r, err := sf.WriteInvalid(messages)
	assert.Nil(r)
	assert.Nil(err)
}

func TestEventForwardingFailure_WriteInvalidApiError(t *testing.T) {
	assert := assert.New(t)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"processor": map[string]string{
				"artifact": "test",
				"version":  "0.1.0",
			},
			"payload": "EventForwarding",
			"failure": map[string]string{
				"latestState":  "Hello EventForwarding!!",
				"timestamp":    "0001-01-01T00:00:00Z",
				"errorType":    "api",
				"errorMessage": "HTTP Status Code: 401 Body: unauthorised",
				"errorCode":    "401",
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	onWrite := func(messages []*models.Message) (*models.TargetWriteResult, error) {
		assert.Equal(5, len(messages))
		for _, msg := range messages {
			diff, err := testutil.GetJsonDiff(string(expectedJSONString), string(msg.Data))
			assert.Nil(err)
			assert.Zero(diff)
		}

		return nil, nil
	}
	tft := TestFailureTarget{
		onWrite: onWrite,
	}

	sf, err := NewEventForwardingFailure(&tft, "test", "0.1.0")
	assert.Nil(err)
	assert.NotNil(sf)

	defer sf.Close()
	sf.Open()

	messages := testutil.GetTestMessages(5, "Hello EventForwarding!!", nil)
	for _, msg := range messages {
		msg.OriginalData = []byte("EventForwarding")
		msg.SetError(&models.ApiError{
			StatusCode:   "401",
			ResponseBody: "unauthorised",
		})
	}

	r, err := sf.WriteInvalid(messages)
	assert.Nil(r)
	assert.Nil(err)
}
