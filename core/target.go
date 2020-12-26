// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// WriteResult contains the results from a target write operation
type WriteResult struct {
	Sent   int64
	Failed int64
}

// Total returns the sum of Sent + Failed messages
func (wr *WriteResult) Total() int64 {
	return wr.Sent + wr.Failed
}

// Target describes the interface for how to push the data pulled from the source
type Target interface {
	Write(events []*Event) (*WriteResult, error)
	Close()
}
