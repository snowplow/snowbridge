// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"time"
)

// MetricsBuffer contains all the metrics we are processing
type MetricsBuffer struct {
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

func (b *MetricsBuffer) Append(res *TargetWriteResult) {
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

func (b *MetricsBuffer) GetAvgProcLatency() time.Duration {
	return b.getAvgLatency(b.SumProcLatency)
}

func (b *MetricsBuffer) GetAvgMessageLatency() time.Duration {
	return b.getAvgLatency(b.SumMessageLatency)
}

func (b *MetricsBuffer) getAvgLatency(sum time.Duration) time.Duration {
	if b.MsgTotal > 0 {
		return time.Duration(int64(sum)/b.MsgTotal) * time.Nanosecond
	}
	return time.Duration(0)
}
