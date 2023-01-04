//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package badrows

import (
	"time"
)

const (
	// Definition: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0
	genericErrorSchema = "iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0"
)

// GenericErrorInput provides the inputs for generating a new generic-error
type GenericErrorInput struct {
	ProcessorArtifact string
	ProcessorVersion  string
	Payload           []byte
	FailureTimestamp  time.Time
	FailureErrors     []string
}

// NewGenericError will build a new generic-error JSON that can be emitted to a Snowplow badrows stream
func NewGenericError(input *GenericErrorInput, targetByteLimit int) (*BadRow, error) {
	fe := make([]string, 0)
	if input.FailureErrors != nil {
		fe = input.FailureErrors
	}

	data := map[string]interface{}{
		dataKeyProcessor: map[string]string{
			"artifact": input.ProcessorArtifact,
			"version":  input.ProcessorVersion,
		},
		dataKeyFailure: map[string]interface{}{
			"timestamp": formatTimeISO8601(input.FailureTimestamp),
			"errors":    fe,
		},
	}

	return newBadRow(
		genericErrorSchema,
		data,
		input.Payload,
		targetByteLimit,
	)
}
