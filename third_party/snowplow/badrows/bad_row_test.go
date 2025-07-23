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

package badrows

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBadRow_InvalidData(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]any{
		"hello": map[bool]string{
			true: "pv",
		},
	}

	br, err := newBadRow(schema, data, []byte("Hello World!"), 5000)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Could not unmarshall bad-row data blob to JSON: json: unsupported type: map[bool]string", err.Error())
	}
	assert.Nil(br)
}

func TestNewBadRowEventForwardingError_InvalidData(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]any{
		"hello": map[bool]string{
			true: "pv",
		},
	}

	payload := map[string]string{
		dataKeyOriginalTSV: "test",
		dataKeyLatestState: "test",
	}

	br, err := newBadRowEventForwardingError(schema, data, payload, 5000)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Could not unmarshall bad-row data blob to JSON: json: unsupported type: map[bool]string", err.Error())
	}
	assert.Nil(br)
}

func TestNewBadRowEventForwardingError_Success(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]any{
		"processor": map[string]string{
			"artifact": "snowbridge",
			"version":  "0.1.0",
		},
		"failure": map[string]string{
			"timestamp": "2023-01-01T00:00:00Z",
			"errorType": "test",
		},
	}

	payload := map[string]string{
		dataKeyOriginalTSV: "original data",
		dataKeyLatestState: "latest data",
	}

	br, err := newBadRowEventForwardingError(schema, data, payload, 5000)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err := br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)
	assert.Contains(compact, "original data")
	assert.Contains(compact, "latest data")
}

func TestNewBadRowEventForwardingError_ByteLimitExceeded(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]any{
		"processor": map[string]string{
			"artifact": "snowbridge",
			"version":  "0.1.0",
		},
		"failure": map[string]string{
			"timestamp": "2023-01-01T00:00:00Z",
			"errorType": "test",
		},
	}

	payload := map[string]string{
		dataKeyOriginalTSV: "original data",
		dataKeyLatestState: "latest data",
	}

	// Use a very small byte limit to trigger the error
	br, err := newBadRowEventForwardingError(schema, data, payload, 10)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to create bad-row as resultant payload will exceed the targets byte limit", err.Error())
	}
	assert.Nil(br)
}
