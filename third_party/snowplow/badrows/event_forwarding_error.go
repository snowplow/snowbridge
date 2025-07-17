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
	// Definition: https://github.com/snowplow/iglu-central/blob/1a5e65b89ec40a3365fc9d99160955ed87c998be/schemas/com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0
	eventForwardingViolationSchema = "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0"
)

// EventForwardingErrorInput provides the inputs for generating a new event forwarding error
type EventForwardingErrorInput struct {
	ProcessorArtifact string
	ProcessorVersion  string
	OriginalTSV       []byte
	ErrorType         string
	// The latest transformed version of the original TSV message
	LatestState      []byte
	ErrorMessage     string
	ErrorCode        string
	FailureTimestamp time.Time
}

// NewEventForwardingError will build a new event forwarding error JSON that can be emitted to a Snowplow badrows stream
func NewEventForwardingError(input *EventForwardingErrorInput, targetByteLimit int) (*BadRow, error) {
	data := map[string]any{
		dataKeyProcessor: map[string]string{
			"artifact": input.ProcessorArtifact,
			"version":  input.ProcessorVersion,
		},
		dataKeyErrorType:    input.ErrorType,
		dataKeyOriginalTSV:  string(input.OriginalTSV),
		dataKeyLatestState:  string(input.LatestState),
		dataKeyErrorMessage: input.ErrorMessage,
		dataKeyErrorCode:    input.ErrorCode,
		dataKeyTimestamp:    formatTimeISO8601(input.FailureTimestamp),
	}

	return newBadRow(
		eventForwardingViolationSchema,
		data,
		input.OriginalTSV,
		targetByteLimit,
	)
}
