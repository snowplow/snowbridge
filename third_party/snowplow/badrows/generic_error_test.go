// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package badrows

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewGenericError(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewGenericError(
		&GenericErrorInput{
			ProcessorArtifact: "stream-replicator",
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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"errors\":[],\"timestamp\":\"%s\"},\"payload\":\"\\u0001\",\"processor\":{\"artifact\":\"stream-replicator\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}

func TestNewGenericError_WithErrors(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewGenericError(
		&GenericErrorInput{
			ProcessorArtifact: "stream-replicator",
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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"errors\":[\"hello!\"],\"timestamp\":\"%s\"},\"payload\":\"\\u0001\",\"processor\":{\"artifact\":\"stream-replicator\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}
