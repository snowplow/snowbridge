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
