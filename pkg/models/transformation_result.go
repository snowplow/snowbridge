// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

type TransformationResult struct {
	ResultCount  int64
	InvalidCount int64

	// Result holds all the messages that were successfully transformed and
	// are ready for attempts to send to the target
	Result []*Message

	// Invalid contains all the messages that cannot be transformed
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message
}

// NewTransformationResult contains slices successfully tranformed and unsuccessfully transformed messages, and their lengths.
func NewTransformationResult(result []*Message, invalid []*Message) *TransformationResult {
	r := TransformationResult{
		int64(len(result)),
		int64(len(invalid)),
		result,
		invalid,
	}
	return &r
}
