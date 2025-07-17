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

	data := map[string]any{
		dataKeyProcessor: map[string]string{
			"artifact": input.ProcessorArtifact,
			"version":  input.ProcessorVersion,
		},
		dataKeyFailure: map[string]any{
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
