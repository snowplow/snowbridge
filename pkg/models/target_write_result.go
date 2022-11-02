// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"time"

	"github.com/snowplow/snowbridge/pkg/common"
)

// TargetWriteResult contains the results from a target write operation
type TargetWriteResult struct {
	SentCount   int64
	FailedCount int64

	// Sent holds all the messages that were successfully sent to the target
	// and therefore have been acked by the target successfully.
	Sent []*Message

	// Failed contains all the messages that could not be sent to the target
	// and that should be retried.
	Failed []*Message

	// Oversized holds all the messages that were too big to be sent to
	// the target and need to be handled externally to the target.
	Oversized []*Message

	// Invalid contains all the messages that cannot be sent to the target
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message

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

	// Delta between TimePulled and TimeTransformed tells us how well the
	// application is at executing transformation functions
	MaxTransformLatency time.Duration
	MinTransformLatency time.Duration
	AvgTransformLatency time.Duration
}

// NewTargetWriteResult uses the current time as the WriteTime and then calls NewTargetWriteResultWithTime
func NewTargetWriteResult(sent []*Message, failed []*Message, oversized []*Message, invalid []*Message) *TargetWriteResult {
	return NewTargetWriteResultWithTime(sent, failed, oversized, invalid, time.Now().UTC())
}

// NewTargetWriteResultWithTime builds a result structure to return from a target write
// attempt which contains the sent and failed message counts as well as several
// derived latency measures.
func NewTargetWriteResultWithTime(sent []*Message, failed []*Message, oversized []*Message, invalid []*Message, timeOfWrite time.Time) *TargetWriteResult {
	r := TargetWriteResult{
		SentCount:   int64(len(sent)),
		FailedCount: int64(len(failed)),
		Sent:        sent,
		Failed:      failed,
		Oversized:   oversized,
		Invalid:     invalid,
	}

	// Calculate latency on sent & failed events
	processed := append(sent, failed...)
	processedLen := int64(len(processed))

	var sumProcLatency time.Duration
	var sumMessageLatency time.Duration
	var sumTransformLatency time.Duration

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

		var transformLatency time.Duration
		if !msg.TimeTransformed.IsZero() {
			transformLatency = msg.TimeTransformed.Sub(msg.TimePulled)
		}
		if r.MaxTransformLatency < transformLatency {
			r.MaxTransformLatency = transformLatency
		}
		if r.MinTransformLatency > transformLatency || r.MinTransformLatency == time.Duration(0) {
			r.MinTransformLatency = transformLatency
		}
		sumTransformLatency += transformLatency
	}

	if processedLen > 0 {
		r.AvgProcLatency = common.GetAverageFromDuration(sumProcLatency, processedLen)
		r.AvgMsgLatency = common.GetAverageFromDuration(sumMessageLatency, processedLen)
		r.AvgTransformLatency = common.GetAverageFromDuration(sumTransformLatency, processedLen)
	}

	return &r
}

// Total returns the sum of Sent + Failed messages
func (wr *TargetWriteResult) Total() int64 {
	return wr.SentCount + wr.FailedCount
}

// Append will add another write result to the source one to allow for
// result concatenation and then return the resultant struct
func (wr *TargetWriteResult) Append(nwr *TargetWriteResult) *TargetWriteResult {
	wrC := *wr

	if nwr != nil {
		wrC.SentCount += nwr.SentCount
		wrC.FailedCount += nwr.FailedCount

		wrC.Sent = append(wrC.Sent, nwr.Sent...)
		wrC.Failed = append(wrC.Failed, nwr.Failed...)
		wrC.Oversized = append(wrC.Oversized, nwr.Oversized...)
		wrC.Invalid = append(wrC.Invalid, nwr.Invalid...)

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

		if wrC.MaxTransformLatency < nwr.MaxTransformLatency {
			wrC.MaxTransformLatency = nwr.MaxTransformLatency
		}
		if wrC.MinTransformLatency > nwr.MinTransformLatency || wrC.MinTransformLatency == time.Duration(0) {
			wrC.MinTransformLatency = nwr.MinTransformLatency
		}
		wrC.AvgTransformLatency = common.GetAverageFromDuration(wrC.AvgTransformLatency+nwr.AvgTransformLatency, 2)
	}

	return &wrC
}
