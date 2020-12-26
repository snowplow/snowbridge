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

// --- Helpers

// toChunkedEvents can be used to make limited slices of events for batching output into smaller requests
func toChunkedEvents(events []*Event, chunkSize int) [][]*Event {
	var divided [][]*Event
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		divided = append(divided, events[i:end])
	}
	return divided
}
