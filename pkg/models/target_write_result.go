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

	"github.com/snowplow/snowbridge/v3/pkg/common"
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

	// Delta between TimePulled and TimeRequestfinished tells us how well the
	// application is at processing data internally
	MaxProcLatency time.Duration
	MinProcLatency time.Duration
	AvgProcLatency time.Duration

	// Delta between TimeCreated and TimeRequestfinished tells us how far behind
	// the application is on the stream it is consuming from
	MaxMsgLatency time.Duration
	MinMsgLatency time.Duration
	AvgMsgLatency time.Duration

	// Delta between TimePulled and TimeTransformed tells us how well the
	// application is at executing transformation functions
	MaxTransformLatency time.Duration
	MinTransformLatency time.Duration
	AvgTransformLatency time.Duration

	// Delta between RequestStarted and RequestFinished gives us the latency of the request.
	MaxRequestLatency time.Duration
	MinRequestLatency time.Duration
	AvgRequestLatency time.Duration

	MaxE2ELatency time.Duration
	MinE2ELatency time.Duration
	AvgE2ELatency time.Duration
}

// NewTargetWriteResult builds a result structure to return from a target write
// attempt which contains the sent and failed message counts as well as several
// derived latency measures.
func NewTargetWriteResult(sent []*Message, failed []*Message, oversized []*Message, invalid []*Message) *TargetWriteResult {
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
	var sumRequestLatency time.Duration
	var sumE2ELatency time.Duration

	for _, msg := range processed {
		procLatency := msg.TimeRequestFinished.Sub(msg.TimePulled)
		if r.MaxProcLatency < procLatency {
			r.MaxProcLatency = procLatency
		}
		if r.MinProcLatency > procLatency || r.MinProcLatency == time.Duration(0) {
			r.MinProcLatency = procLatency
		}
		sumProcLatency += procLatency

		messageLatency := msg.TimeRequestFinished.Sub(msg.TimeCreated)
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

		requestLatency := msg.TimeRequestFinished.Sub(msg.TimeRequestStarted)
		if r.MaxRequestLatency < requestLatency {
			r.MaxRequestLatency = requestLatency
		}
		if r.MinRequestLatency > requestLatency || r.MinRequestLatency == time.Duration(0) {
			r.MinRequestLatency = requestLatency
		}
		sumRequestLatency += requestLatency

		var e2eLatency time.Duration
		if !msg.CollectorTstamp.IsZero() {
			e2eLatency = msg.TimeRequestFinished.Sub(msg.CollectorTstamp)
		}
		if r.MaxE2ELatency < e2eLatency {
			r.MaxE2ELatency = e2eLatency
		}
		if r.MinE2ELatency > e2eLatency || r.MinE2ELatency == time.Duration(0) {
			r.MinE2ELatency = e2eLatency
		}
		sumE2ELatency += e2eLatency
	}

	if processedLen > 0 {
		r.AvgProcLatency = common.GetAverageFromDuration(sumProcLatency, processedLen)
		r.AvgMsgLatency = common.GetAverageFromDuration(sumMessageLatency, processedLen)
		r.AvgTransformLatency = common.GetAverageFromDuration(sumTransformLatency, processedLen)
		r.AvgRequestLatency = common.GetAverageFromDuration(sumRequestLatency, processedLen)
		r.AvgE2ELatency = common.GetAverageFromDuration(sumE2ELatency, processedLen)
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

		if wrC.MaxRequestLatency < nwr.MaxRequestLatency {
			wrC.MaxRequestLatency = nwr.MaxRequestLatency
		}
		if wrC.MinRequestLatency > nwr.MinRequestLatency || wrC.MinRequestLatency == time.Duration(0) {
			wrC.MinRequestLatency = nwr.MinRequestLatency
		}
		wrC.AvgRequestLatency = common.GetAverageFromDuration(wrC.AvgRequestLatency+nwr.AvgRequestLatency, 2)

		if wrC.MaxE2ELatency < nwr.MaxE2ELatency {
			wrC.MaxE2ELatency = nwr.MaxE2ELatency
		}
		if wrC.MinE2ELatency > nwr.MinE2ELatency || wrC.MinE2ELatency == time.Duration(0) {
			wrC.MinE2ELatency = nwr.MinE2ELatency
		}
		wrC.AvgE2ELatency = common.GetAverageFromDuration(wrC.AvgE2ELatency+nwr.AvgE2ELatency, 2)
	}

	return &wrC
}
