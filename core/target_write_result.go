// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"time"
)

// TargetWriteResult contains the results from a target write operation
type TargetWriteResult struct {
	Sent   int64
	Failed int64

	// Delta between TimePulled and TimeOfWrite tells us how well the
	// application is at processing data internally
	MaxProcLatency time.Duration
	MinProcLatency time.Duration
	AvgProcLatency time.Duration

	// Delta between TimeCreated and TimeOfWrite tells us how far behind
	// the application is on the stream it is consuming from
	MaxMessageLatency time.Duration
	MinMessageLatency time.Duration
	AvgMessageLatency time.Duration
}

// NewWriteResult uses the current time as the WriteTime and then calls NewWriteResultWithTime
func NewWriteResult(sent int64, failed int64, messages []*Event) *TargetWriteResult {
	return NewWriteResultWithTime(sent, failed, time.Now().UTC(), messages)
}

// NewWriteResultWithTime builds a result structure to return from a target write
// attempt which contains the sent and failed message counts as well as several
// derived latency measures.
func NewWriteResultWithTime(sent int64, failed int64, timeOfWrite time.Time, messages []*Event) *TargetWriteResult {
	messagesLen := int64(len(messages))

	var maxProcLatency time.Duration
	var minProcLatency time.Duration
	var avgProcLatency time.Duration
	var sumProcLatency time.Duration

	var maxMessageLatency time.Duration
	var minMessageLatency time.Duration
	var avgMessageLatency time.Duration
	var sumMessageLatency time.Duration

	for _, msg := range messages {
		procLatency := timeOfWrite.Sub(msg.TimePulled)
		if maxProcLatency < procLatency {
			maxProcLatency = procLatency
		}
		if minProcLatency > procLatency || minProcLatency == time.Duration(0) {
			minProcLatency = procLatency
		}
		sumProcLatency += procLatency

		messageLatency := timeOfWrite.Sub(msg.TimeCreated)
		if maxMessageLatency < messageLatency {
			maxMessageLatency = messageLatency
		}
		if minMessageLatency > messageLatency || minMessageLatency == time.Duration(0) {
			minMessageLatency = messageLatency
		}
		sumMessageLatency += messageLatency
	}

	if messagesLen > 0 {
		avgProcLatency = time.Duration(int64(sumProcLatency)/messagesLen) * time.Nanosecond
		avgMessageLatency = time.Duration(int64(sumMessageLatency)/messagesLen) * time.Nanosecond
	}

	return &TargetWriteResult{
		Sent:              sent,
		Failed:            failed,
		MaxProcLatency:    maxProcLatency,
		MinProcLatency:    minProcLatency,
		AvgProcLatency:    avgProcLatency,
		MaxMessageLatency: maxMessageLatency,
		MinMessageLatency: minMessageLatency,
		AvgMessageLatency: avgMessageLatency,
	}
}

// Total returns the sum of Sent + Failed messages
func (wr *TargetWriteResult) Total() int64 {
	return wr.Sent + wr.Failed
}
