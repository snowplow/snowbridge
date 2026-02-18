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
	// Transformed holds the message that was successfully transformed and
	// is ready for attempts to send to the target
	Transformed *Message

	// Filtered holds the message that was designated to be filtered out
	// it will be acked without passing through to any target
	Filtered *Message

	// Invalid contains the message that cannot be transformed
	// due to various parseability reasons.  This message cannot be retried
	// and needs to be specially handled.
	Invalid *Message
}

// NewTransformationResult creates a new TransformationResult with the provided transformed, filtered and invalid messages.
func NewTransformationResult(transformed *Message, filtered *Message, invalid *Message) *TransformationResult {
	r := TransformationResult{
		transformed,
		filtered,
		invalid,
	}
	return &r
}
