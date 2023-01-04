//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
			ProcessorArtifact:              "snowbridge",
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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"actualSizeBytes\":12,\"expectation\":\"Not sure?\",\"maximumAllowedSizeBytes\":20,\"timestamp\":\"%s\"},\"payload\":\"Hello World!\",\"processor\":{\"artifact\":\"snowbridge\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}

func TestNewSizeViolation_Truncated(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewSizeViolation(
		&SizeViolationInput{
			ProcessorArtifact:              "snowbridge",
			ProcessorVersion:               "0.1.0",
			Payload:                        []byte("Hello World! This is a longer string than before, because the processor name is shorter than before!"),
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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"actualSizeBytes\":100,\"expectation\":\"Not sure?\",\"maximumAllowedSizeBytes\":20,\"timestamp\":\"%s\"},\"payload\":\"Hello World! Th\",\"processor\":{\"artifact\":\"snowbridge\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}

func TestNewSizeViolation_NotEnoughBytes(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewSizeViolation(
		&SizeViolationInput{
			ProcessorArtifact:              "snowbridge",
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
