// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"time"
	"fmt"
)

// ObserverBuffer contains all the metrics we are processing
type ObserverBuffer struct {
	MsgSent           int64
	MsgFailed         int64
	MsgTotal          int64
	MaxProcLatency    time.Duration
	MinProcLatency    time.Duration
	SumProcLatency    time.Duration
	MaxMessageLatency time.Duration
	MinMessageLatency time.Duration
	SumMessageLatency time.Duration
}

// Append adds a TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) Append(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.MsgSent += res.Sent
	b.MsgFailed += res.Failed
	b.MsgTotal += res.Total()

	if b.MaxProcLatency < res.MaxProcLatency {
		b.MaxProcLatency = res.MaxProcLatency
	}
	if b.MinProcLatency > res.MinProcLatency {
		b.MinProcLatency = res.MinProcLatency
	}
	b.SumProcLatency += res.AvgProcLatency

	if b.MaxMessageLatency < res.MaxMessageLatency {
		b.MaxMessageLatency = res.MaxMessageLatency
	}
	if b.MinMessageLatency > res.MinMessageLatency {
		b.MinMessageLatency = res.MinMessageLatency
	}
	b.SumMessageLatency += res.AvgMessageLatency
}

// GetAvgProcLatency calculates average processing latency
func (b *ObserverBuffer) GetAvgProcLatency() time.Duration {
	return getAverageFromDuration(b.SumProcLatency, b.MsgTotal)
}

// GetAvgMessageLatency calculates average message latency
func (b *ObserverBuffer) GetAvgMessageLatency() time.Duration {
	return getAverageFromDuration(b.SumMessageLatency, b.MsgTotal)
}

func (b *ObserverBuffer) String() string {
	avgProcLatency := b.GetAvgProcLatency()
	avgMessageLatency := b.GetAvgMessageLatency()

	return fmt.Sprintf(
		"MsgSent:%d,MsgFailed:%d,MsgTotal:%d,MaxProcLatency:%s,MinProcLatency:%s,AvgProcLatency:%s,MaxMessageLatency:%s,MinMessageLatency:%s,AvgMessageLatency:%s",
		b.MsgSent,
		b.MsgFailed,
		b.MsgTotal,
		b.MaxProcLatency,
		b.MinProcLatency,
		avgProcLatency,
		b.MaxMessageLatency,
		b.MinMessageLatency,
		avgMessageLatency,
	)
}
