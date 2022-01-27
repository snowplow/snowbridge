// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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

	return NewBadRow(
		genericErrorSchema,
		data,
		input.Payload,
		targetByteLimit,
	)
}
