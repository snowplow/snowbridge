// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// SQSTarget holds a new client for writing events to sqs
type SQSTarget struct{}

// NewSQSTarget creates a new client for writing events to sqs
func NewSQSTarget() (*SQSTarget, error) {
	return &SQSTarget{}, nil
}

// Write pushes all events to the required target
func (st *SQSTarget) Write(events []*Event) error {
	return nil
}

// Close does not do anything for this target
func (st *SQSTarget) Close() {}
