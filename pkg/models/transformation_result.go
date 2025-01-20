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

package models

// TransformationResult contains the results from a transformation operation
type TransformationResult struct {
	ResultCount   int64
	FilteredCount int64
	InvalidCount  int64

	// Result holds all the messages that were successfully transformed and
	// are ready for attempts to send to the target
	Result []*Message

	// Filtered holds all the messages that were designated to be filtered out
	// they will all be acked without passing through to any target
	Filtered []*Message

	// Invalid contains all the messages that cannot be transformed
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message
}

// NewTransformationResult contains slices successfully tranformed, filtered and unsuccessfully transformed messages, and their lengths.
func NewTransformationResult(result []*Message, filtered []*Message, invalid []*Message) *TransformationResult {
	r := TransformationResult{
		int64(len(result)),
		int64(len(filtered)),
		int64(len(invalid)),
		result,
		filtered,
		invalid,
	}
	return &r
}
