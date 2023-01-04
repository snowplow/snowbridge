//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package models

import (
	"time"

	"github.com/snowplow/snowbridge/pkg/common"
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

// NewFilterResult uses the current time as the timeOfFilter and calls newFilterResultWithTime
func NewFilterResult(filtered []*Message) *FilterResult {
	return newFilterResultWithTime(filtered, time.Now().UTC())
}

// newFilterResultWithTime builds a result structure to return from a filtered message slice
// attempt which contains the filtered message count as well as several
// derived latency measures.
func newFilterResultWithTime(filtered []*Message, timeOfFilter time.Time) *FilterResult {
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
