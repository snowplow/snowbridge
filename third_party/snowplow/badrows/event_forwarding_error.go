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
	// Definition: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1
	eventForwardingViolationSchema = "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1"
)

// EventForwardingErrorInput provides the inputs for generating a new event forwarding error
type EventForwardingErrorInput struct {
	ProcessorArtifact string
	ProcessorVersion  string
	// Optional identifiers for the forwarder that produced this error
	ForwarderID   string
	ForwarderName string
	OriginalTSV   []byte
	ErrorType     string
	// The latest transformed version of the original TSV message
	LatestState      []byte
	ErrorMessage     string
	ErrorCode        string
	FailureTimestamp time.Time
}

// NewEventForwardingError will build a new event forwarding error JSON that can be emitted to a Snowplow badrows stream
func NewEventForwardingError(input *EventForwardingErrorInput, targetByteLimit int) (*BadRow, error) {
	// Processor information, including the forwarder identifiers when configured
	processor := map[string]string{
		"artifact": input.ProcessorArtifact,
		"version":  input.ProcessorVersion,
	}
	if input.ForwarderID != "" {
		processor["id"] = input.ForwarderID
	}
	if input.ForwarderName != "" {
		processor["name"] = input.ForwarderName
	}

	data := map[string]any{
		dataKeyProcessor: processor,

		// Failure information
		dataKeyFailure: map[string]string{
			dataKeyTimestamp:    formatTimeISO8601(input.FailureTimestamp),
			dataKeyErrorType:    input.ErrorType,
			dataKeyErrorMessage: input.ErrorMessage,
			dataKeyErrorCode:    input.ErrorCode,
		},
	}

	return newBadRowEventForwardingError(
		eventForwardingViolationSchema,
		data,
		input.OriginalTSV,
		input.LatestState,
		targetByteLimit,
	)
}
