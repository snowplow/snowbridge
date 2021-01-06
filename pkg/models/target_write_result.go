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

// TargetWriteResult contains the results from a target write operation
type TargetWriteResult struct {
	Sent   int64
	Failed int64

	// Oversized holds all the messages that were too big to be sent to
	// the downstream target
	Oversized []*Message

	// Delta between TimePulled and TimeOfWrite tells us how well the
	// application is at processing data internally
	MaxProcLatency time.Duration
	MinProcLatency time.Duration
	AvgProcLatency time.Duration

	// Delta between TimeCreated and TimeOfWrite tells us how far behind
	// the application is on the stream it is consuming from
	MaxMsgLatency time.Duration
	MinMsgLatency time.Duration
	AvgMsgLatency time.Duration
}

// NewTargetWriteResult uses the current time as the WriteTime and then calls NewTargetWriteResultWithTime
func NewTargetWriteResult(sent int64, failed int64, processed []*Message, oversized []*Message) *TargetWriteResult {
	return NewTargetWriteResultWithTime(sent, failed, time.Now().UTC(), processed, oversized)
}

// NewTargetWriteResultWithTime builds a result structure to return from a target write
// attempt which contains the sent and failed message counts as well as several
// derived latency measures.
func NewTargetWriteResultWithTime(sent int64, failed int64, timeOfWrite time.Time, processed []*Message, oversized []*Message) *TargetWriteResult {
	r := TargetWriteResult{
		Sent:      sent,
		Failed:    failed,
		Oversized: oversized,
	}

	processedLen := int64(len(processed))

	var sumProcLatency time.Duration
	var sumMessageLatency time.Duration

	for _, msg := range processed {
		procLatency := timeOfWrite.Sub(msg.TimePulled)
		if r.MaxProcLatency < procLatency {
			r.MaxProcLatency = procLatency
		}
		if r.MinProcLatency > procLatency || r.MinProcLatency == time.Duration(0) {
			r.MinProcLatency = procLatency
		}
		sumProcLatency += procLatency

		messageLatency := timeOfWrite.Sub(msg.TimeCreated)
		if r.MaxMsgLatency < messageLatency {
			r.MaxMsgLatency = messageLatency
		}
		if r.MinMsgLatency > messageLatency || r.MinMsgLatency == time.Duration(0) {
			r.MinMsgLatency = messageLatency
		}
		sumMessageLatency += messageLatency
	}

	if processedLen > 0 {
		r.AvgProcLatency = common.GetAverageFromDuration(sumProcLatency, processedLen)
		r.AvgMsgLatency = common.GetAverageFromDuration(sumMessageLatency, processedLen)
	}

	return &r
}

// Total returns the sum of Sent + Failed messages
func (wr *TargetWriteResult) Total() int64 {
	return wr.Sent + wr.Failed
}

// Append will add another write result to the source one to allow for
// result concatenation and then return the resultant struct
func (wr *TargetWriteResult) Append(nwr *TargetWriteResult) *TargetWriteResult {
	wrC := *wr

	if nwr != nil {
		wrC.Sent += nwr.Sent
		wrC.Failed += nwr.Failed
		wrC.Oversized = append(wrC.Oversized, nwr.Oversized...)

		if wrC.MaxProcLatency < nwr.MaxProcLatency {
			wrC.MaxProcLatency = nwr.MaxProcLatency
		}
		if wrC.MinProcLatency > nwr.MinProcLatency || wrC.MinProcLatency == time.Duration(0) {
			wrC.MinProcLatency = nwr.MinProcLatency
		}
		wrC.AvgProcLatency = common.GetAverageFromDuration(wrC.AvgProcLatency+nwr.AvgProcLatency, 2)

		if wrC.MaxMsgLatency < nwr.MaxMsgLatency {
			wrC.MaxMsgLatency = nwr.MaxMsgLatency
		}
		if wrC.MinMsgLatency > nwr.MinMsgLatency || wrC.MinMsgLatency == time.Duration(0) {
			wrC.MinMsgLatency = nwr.MinMsgLatency
		}
		wrC.AvgMsgLatency = common.GetAverageFromDuration(wrC.AvgMsgLatency+nwr.AvgMsgLatency, 2)
	}

	return &wrC
}
