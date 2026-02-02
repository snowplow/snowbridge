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

func TestNewBadRow_Success(t *testing.T) {
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

	payload := []byte("Hello World!")
	br, err := newBadRow(schema, data, payload, 5000)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err := br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)
	assert.Contains(compact, "Hello World!")
	assert.Contains(compact, schema)
}

func TestNewBadRow_PayloadTruncation(t *testing.T) {
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

	byteLimit := 200

	payload := []byte("This is a very long payload that should be truncated")

	// Set byte limit small enough to trigger truncation but large enough to fit some payload
	br, err := newBadRow(schema, data, payload, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err := br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should contain partial payload, not the full payload
	assert.Contains(compact, `"payload":"This is"`)
	assert.LessOrEqual(len(compact), byteLimit)

	// Check non UTF8 payload gets trimmed correctly
	payload = []byte("君が代は千代に八千代にさざれ石の巌となり")
	br, err = newBadRow(schema, data, payload, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err = br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should contain partial payload, not the full payload
	assert.Contains(compact, `"payload":"君が"`)
	assert.LessOrEqual(len(compact), byteLimit)

	// Check special charactrers payload gets trimmed correctly
	payload = []byte(`\ufffd\ufffd\ufffd\ufffd\ufffd\`)
	br, err = newBadRow(schema, data, payload, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err = br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should contain partial payload, not the full payload
	assert.Contains(compact, `"payload":"\\ufffd"`)
	assert.LessOrEqual(len(compact), byteLimit)
}

func TestNewBadRow_ByteLimitExceeded(t *testing.T) {
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

	payload := []byte("Hello World!")
	// Use a very small byte limit to trigger the error
	br, err := newBadRow(schema, data, payload, 10)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to create bad-row as resultant payload will exceed the targets byte limit", err.Error())
	}
	assert.Nil(br)
}

func TestNewBadRow_EmptyPayload(t *testing.T) {
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

	payload := []byte("")
	br, err := newBadRow(schema, data, payload, 5000)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err := br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)
	// Should have empty payload
	assert.Contains(compact, `"payload":""`)
	assert.Contains(compact, schema)
}

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

	br, err := newBadRowEventForwardingError(schema, data, []byte("original data"), []byte("latest data"), 5000)
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

	// Use a very small byte limit to trigger the error
	br, err := newBadRowEventForwardingError(schema, data, []byte("original data"), []byte("latest data"), 10)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to create bad-row as resultant payload will exceed the targets byte limit", err.Error())
	}
	assert.Nil(br)
}

func TestNewBadRowEventForwardingError_PayloadTruncation(t *testing.T) {
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

	byteLimit := 250

	payload := []byte("This is a very long payload that should be truncated")
	latestState := []byte("This is some latest state data")

	// Set byte limit small enough to trigger truncation but large enough to fit some payload
	br, err := newBadRowEventForwardingError(schema, data, payload, latestState, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err := br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should contain partial payload (latestState gets truncated first)
	assert.Contains(compact, `"payload":"This is a very long payload that sho"`)
	assert.Contains(compact, `"latestState":""`)
	assert.LessOrEqual(len(compact), byteLimit)

	// Check UTF-8 payload gets trimmed correctly
	payload = []byte("君が代は千代に八千代にさざれ石の巌となり")
	latestState = []byte("状態データ")
	br, err = newBadRowEventForwardingError(schema, data, payload, latestState, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err = br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should contain partial payload with proper UTF-8 truncation
	assert.Contains(compact, `"payload":"君が代は千代に八千代にさ"`)
	assert.Contains(compact, `"latestState":""`)
	assert.LessOrEqual(len(compact), byteLimit)

	// Check special characters payload gets trimmed correctly
	payload = []byte(`\ufffd\ufffd\ufffd\ufffd\ufffd\`)
	latestState = []byte(`\special\data\`)
	br, err = newBadRowEventForwardingError(schema, data, payload, latestState, byteLimit)
	assert.Nil(err)
	assert.NotNil(br)

	compact, err = br.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	// Should handle special characters properly with truncation (latestState truncated first)
	assert.Contains(compact, `"payload":"\\ufffd\\ufffd\\ufffd\\ufffd\\ufffd\\"`)
	assert.Contains(compact, `"latestState":"\\s"`)
	assert.LessOrEqual(len(compact), byteLimit)
}
