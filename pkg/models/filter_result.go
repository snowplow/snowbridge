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
