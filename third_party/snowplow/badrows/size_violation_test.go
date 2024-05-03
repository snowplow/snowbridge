/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

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
