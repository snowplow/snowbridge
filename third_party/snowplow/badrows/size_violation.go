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
	// Definition: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0
	sizeViolationSchema = "iglu:com.snowplowanalytics.snowplow.badrows/size_violation/jsonschema/1-0-0"
)

// SizeViolationInput provides the inputs for generating a new size-violation
type SizeViolationInput struct {
	ProcessorArtifact              string
	ProcessorVersion               string
	Payload                        []byte
	FailureTimestamp               time.Time
	FailureMaximumAllowedSizeBytes int
	FailureExpectation             string
}

// NewSizeViolation will build a new size-violation JSON that can be emitted to a Snowplow badrows stream
func NewSizeViolation(input *SizeViolationInput, targetByteLimit int) (*BadRow, error) {
	data := map[string]interface{}{
		dataKeyProcessor: map[string]string{
			"artifact": input.ProcessorArtifact,
			"version":  input.ProcessorVersion,
		},
		dataKeyFailure: map[string]interface{}{
			"timestamp":               formatTimeISO8601(input.FailureTimestamp),
			"maximumAllowedSizeBytes": input.FailureMaximumAllowedSizeBytes,
			"actualSizeBytes":         len(input.Payload),
			"expectation":             input.FailureExpectation,
		},
	}

	return newBadRow(
		sizeViolationSchema,
		data,
		input.Payload,
		targetByteLimit,
	)
}
