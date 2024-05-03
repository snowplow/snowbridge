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
