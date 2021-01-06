// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"fmt"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
)

// ObserverBuffer contains all the metrics we are processing
type ObserverBuffer struct {
	TargetResults int64
	MsgSent       int64
	MsgFailed     int64
	MsgTotal      int64

	OversizedTargetResults int64
	OversizedMsgSent       int64
	OversizedMsgFailed     int64
	OversizedMsgTotal      int64

	MaxProcLatency time.Duration
	MinProcLatency time.Duration
	SumProcLatency time.Duration
	MaxMsgLatency  time.Duration
	MinMsgLatency  time.Duration
	SumMsgLatency  time.Duration
}

// Append adds a TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) Append(res *TargetWriteResult, oversized bool) {
	if res == nil {
		return
	}

	if !oversized {
		b.TargetResults++

		b.MsgSent += res.Sent
		b.MsgFailed += res.Failed
		b.MsgTotal += res.Total()
	} else {
		b.OversizedTargetResults++

		b.OversizedMsgSent += res.Sent
		b.OversizedMsgFailed += res.Failed
		b.OversizedMsgTotal += res.Total()
	}

	if b.MaxProcLatency < res.MaxProcLatency {
		b.MaxProcLatency = res.MaxProcLatency
	}
	if b.MinProcLatency > res.MinProcLatency || b.MinProcLatency == time.Duration(0) {
		b.MinProcLatency = res.MinProcLatency
	}
	b.SumProcLatency += res.AvgProcLatency

	if b.MaxMsgLatency < res.MaxMsgLatency {
		b.MaxMsgLatency = res.MaxMsgLatency
	}
	if b.MinMsgLatency > res.MinMsgLatency || b.MinMsgLatency == time.Duration(0) {
		b.MinMsgLatency = res.MinMsgLatency
	}
	b.SumMsgLatency += res.AvgMsgLatency
}

// GetSumResults returns the total number of results logged in the buffer
func (b *ObserverBuffer) GetSumResults() int64 {
	return b.TargetResults + b.OversizedTargetResults
}

// GetAvgProcLatency calculates average processing latency
func (b *ObserverBuffer) GetAvgProcLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumProcLatency, b.GetSumResults())
}

// GetAvgMsgLatency calculates average message latency
func (b *ObserverBuffer) GetAvgMsgLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumMsgLatency, b.GetSumResults())
}

func (b *ObserverBuffer) String() string {
	return fmt.Sprintf(
		"TargetResults:%d,MsgSent:%d,MsgFailed:%d,OversizedTargetResults:%d,OversizedMsgSent:%d,OversizedMsgFailed:%d,MaxProcLatency:%s,MaxMsgLatency:%s",
		b.TargetResults,
		b.MsgSent,
		b.MsgFailed,
		b.OversizedTargetResults,
		b.OversizedMsgSent,
		b.OversizedMsgFailed,
		b.MaxProcLatency,
		b.MaxMsgLatency,
	)
}
