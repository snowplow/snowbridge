// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

type TransformationResult struct {
	ResultCount int64
	// FilteredCount int64
	InvalidCount int64

	// Result holds all the messages that were successfully transformed and
	// are ready for attempts to send to the target
	Result []*Message

	// Filtered contains all the messages that could be transformed but
	// are filtered out from sending to the target.
	// Filtered []*Message

	// Invalid contains all the messages that cannot be transformed
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message
}

// NewTargetWriteResult uses the current time as the WriteTime and then calls NewTargetWriteResultWithTime
func NewTransformationResult(result []*Message, invalid []*Message) *TransformationResult {
	r := TransformationResult{
		int64(len(result)),
		// int64(len(filtered)),
		int64(len(invalid)),
		result,
		// filtered,
		invalid,
	}
	return &r
}
