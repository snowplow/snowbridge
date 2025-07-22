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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewGenericError(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewGenericError(
		&GenericErrorInput{
			ProcessorArtifact: "snowbridge",
			ProcessorVersion:  "0.1.0",
			Payload:           []byte("\u0001"),
			FailureTimestamp:  timeNow,
			FailureErrors:     nil,
		},
		262144,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"processor": map[string]string{
				"artifact": "snowbridge",
				"version":  "0.1.0",
			},
			"payload": "\u0001",
			"failure": map[string]any{
				"timestamp": timeNow.UTC().Format("2006-01-02T15:04:05Z07:00"),
				"errors":    []string{},
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	assert.Equal(string(expectedJSONString), compact)
}

func TestNewGenericError_WithErrors(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewGenericError(
		&GenericErrorInput{
			ProcessorArtifact: "snowbridge",
			ProcessorVersion:  "0.1.0",
			Payload:           []byte("\u0001"),
			FailureTimestamp:  timeNow,
			FailureErrors:     []string{"hello!"},
		},
		262144,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	expectedJSON := map[string]any{
		"data": map[string]any{
			"processor": map[string]string{
				"artifact": "snowbridge",
				"version":  "0.1.0",
			},
			"payload": "\u0001",
			"failure": map[string]any{
				"timestamp": timeNow.UTC().Format("2006-01-02T15:04:05Z07:00"),
				"errors":    []string{"hello!"},
			},
		},
		"schema": "iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0",
	}

	expectedJSONString, err := json.Marshal(expectedJSON)
	assert.Nil(err)

	assert.Equal(string(expectedJSONString), compact)
}
