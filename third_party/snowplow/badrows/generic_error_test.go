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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"errors\":[],\"timestamp\":\"%s\"},\"payload\":\"\\u0001\",\"processor\":{\"artifact\":\"snowbridge\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
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
	assert.Equal(fmt.Sprintf("{\"data\":{\"failure\":{\"errors\":[\"hello!\"],\"timestamp\":\"%s\"},\"payload\":\"\\u0001\",\"processor\":{\"artifact\":\"snowbridge\",\"version\":\"0.1.0\"}},\"schema\":\"iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0\"}", timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
}
