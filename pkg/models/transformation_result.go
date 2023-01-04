//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
