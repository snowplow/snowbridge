// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
)

// FilterResult contains the results from a target write operation
type FilterResult struct {
	FilteredCount int64

	// Filtered holds all the messages that were filtered out and acked without sending to the target
	Filtered []*Message

	// Delta between TimePulled and TimeOfAck tells us how well the
	// application is at processing filtered data internally
	MaxFilterLatency time.Duration
	MinFilterLatency time.Duration
	AvgFilterLatency time.Duration
}

// NewFilterResult uses the current time as the timeOfFilter and then calls NewFilterResultWithTime
func NewFilterResult(filtered []*Message) *FilterResult {
	return NewFilterResultWithTime(filtered, time.Now().UTC())
}

// NewFilterResultWithTime builds a result structure to return from a filtered message slice
// attempt which contains the sfiltered message count as well as several
// derived latency measures.
func NewFilterResultWithTime(filtered []*Message, timeOfFilter time.Time) *FilterResult {
	r := FilterResult{
		FilteredCount: int64(len(filtered)),
	}

	filteredLen := int64(len(filtered))

	var sumFilterLatency time.Duration

	for _, msg := range filtered {
		filterLatency := timeOfFilter.Sub(msg.TimePulled)
		if r.MaxFilterLatency < filterLatency {
			r.MaxFilterLatency = filterLatency
		}
		if r.MinFilterLatency > filterLatency || r.MinFilterLatency == time.Duration(0) {
			r.MinFilterLatency = filterLatency
		}
		sumFilterLatency += filterLatency
	}

	if filteredLen > 0 {
		r.AvgFilterLatency = common.GetAverageFromDuration(sumFilterLatency, filteredLen)
	}

	return &r
}
