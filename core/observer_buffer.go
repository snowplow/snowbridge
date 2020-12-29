// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"time"
)

// ObserverBuffer contains all the metrics we are processing
type ObserverBuffer struct {
	TargetResults  int64
	MsgSent        int64
	MsgFailed      int64
	MsgTotal       int64
	MaxProcLatency time.Duration
	MinProcLatency time.Duration
	SumProcLatency time.Duration
	MaxMsgLatency  time.Duration
	MinMsgLatency  time.Duration
	SumMsgLatency  time.Duration
}

// Append adds a TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) Append(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.TargetResults++

	b.MsgSent += res.Sent
	b.MsgFailed += res.Failed
	b.MsgTotal += res.Total()

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

// GetAvgProcLatency calculates average processing latency
func (b *ObserverBuffer) GetAvgProcLatency() time.Duration {
	return getAverageFromDuration(b.SumProcLatency, b.MsgTotal)
}

// GetAvgMsgLatency calculates average message latency
func (b *ObserverBuffer) GetAvgMsgLatency() time.Duration {
	return getAverageFromDuration(b.SumMsgLatency, b.MsgTotal)
}

func (b *ObserverBuffer) String() string {
	avgProcLatency := b.GetAvgProcLatency()
	avgMsgLatency := b.GetAvgMsgLatency()

	return fmt.Sprintf(
		"TargetResults:%d,MsgSent:%d,MsgFailed:%d,MsgTotal:%d,MaxProcLatency:%s,MinProcLatency:%s,AvgProcLatency:%s,MaxMsgLatency:%s,MinMsgLatency:%s,AvgMsgLatency:%s",
		b.TargetResults,
		b.MsgSent,
		b.MsgFailed,
		b.MsgTotal,
		b.MaxProcLatency,
		b.MinProcLatency,
		avgProcLatency,
		b.MaxMsgLatency,
		b.MinMsgLatency,
		avgMsgLatency,
	)
}
