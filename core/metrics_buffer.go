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
	msgSent           int64
	msgFailed         int64
	msgTotal          int64
	maxProcLatency    time.Duration
	minProcLatency    time.Duration
	sumProcLatency    time.Duration
	maxMessageLatency time.Duration
	minMessageLatency time.Duration
	sumMessageLatency time.Duration
}

func (b *MetricsBuffer) appendTargetWriteResults(res *TargetWriteResult) {
	if res == nil {
		return
	}
	
	b.msgSent += res.Sent
	b.msgFailed += res.Failed
	b.msgTotal += res.Total()

	if b.maxProcLatency < res.MaxProcLatency {
		b.maxProcLatency = res.MaxProcLatency
	}
	if b.minProcLatency > res.MinProcLatency {
		b.minProcLatency = res.MinProcLatency
	}
	b.sumProcLatency += res.AvgProcLatency

	if b.maxMessageLatency < res.MaxMessageLatency {
		b.maxMessageLatency = res.MaxMessageLatency
	}
	if b.minMessageLatency > res.MinMessageLatency {
		b.minMessageLatency = res.MinMessageLatency
	}
	b.sumMessageLatency += res.AvgMessageLatency
}

func (b *MetricsBuffer) getAvgProcLatency() time.Duration {
	return b.getAvgLatency(b.sumProcLatency)
}

func (b *MetricsBuffer) getAvgMessageLatency() time.Duration {
	return b.getAvgLatency(b.sumMessageLatency)
}

func (b *MetricsBuffer) getAvgLatency(sum time.Duration) time.Duration {
	if b.msgTotal > 0 {
		return time.Duration(int64(sum)/b.msgTotal) * time.Nanosecond
	}
	return time.Duration(0)
}
