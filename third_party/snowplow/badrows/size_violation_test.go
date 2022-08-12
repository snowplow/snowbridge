// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package badrows

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewSizeViolation(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewSizeViolation(
		&SizeViolationInput{
			ProcessorArtifact:              "stream-replicator",
			ProcessorVersion:               "0.1.0",
			Payload:                        []byte("Hello World!"),
			FailureTimestamp:               timeNow,
			FailureMaximumAllowedSizeBytes: 20,
			FailureExpectation:             "Not sure?",
		},
		262144,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"actualSizeBytes\":12,\"expectation\":\"Not sure?\",\"maximumAllowedSizeBytes\":20,\"timestamp\":\"%s\"},\"payload\":\"Hello World!\",\"processor\":{\"artifact\":\"stream-replicator\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}

func TestNewSizeViolation_Truncated(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewSizeViolation(
		&SizeViolationInput{
			ProcessorArtifact:              "stream-replicator",
			ProcessorVersion:               "0.1.0",
			Payload:                        []byte("Hello World!"),
			FailureTimestamp:               timeNow,
			FailureMaximumAllowedSizeBytes: 20,
			FailureExpectation:             "Not sure?",
		},
		305,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"actualSizeBytes\":12,\"expectation\":\"Not sure?\",\"maximumAllowedSizeBytes\":20,\"timestamp\":\"%s\"},\"payload\":\"Hello Wor\",\"processor\":{\"artifact\":\"stream-replicator\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}

func TestNewSizeViolation_NotEnoughBytes(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewSizeViolation(
		&SizeViolationInput{
			ProcessorArtifact:              "stream-replicator",
			ProcessorVersion:               "0.1.0",
			Payload:                        []byte("Hello World!"),
			FailureTimestamp:               timeNow,
			FailureMaximumAllowedSizeBytes: 20,
			FailureExpectation:             "Not sure?",
		},
		10,
	)
	assert.Nil(sv)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to create bad-row as resultant payload will exceed the targets byte limit", err.Error())
	}
}
