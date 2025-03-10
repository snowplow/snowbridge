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
	"fmt"
	"time"

	"github.com/snowplow/snowbridge/pkg/common"
)

// ObserverBuffer contains all the metrics we are processing
type ObserverBuffer struct {
	TargetResults int64
	MsgSent       int64
	MsgFailed     int64
	MsgTotal      int64

	MsgFiltered int64

	OversizedTargetResults int64
	OversizedMsgSent       int64
	OversizedMsgFailed     int64
	OversizedMsgTotal      int64

	InvalidTargetResults int64
	InvalidMsgSent       int64
	InvalidMsgFailed     int64
	InvalidMsgTotal      int64

	MaxProcLatency      time.Duration
	MinProcLatency      time.Duration
	SumProcLatency      time.Duration
	MaxMsgLatency       time.Duration
	MinMsgLatency       time.Duration
	SumMsgLatency       time.Duration
	MaxTransformLatency time.Duration
	MinTransformLatency time.Duration
	SumTransformLatency time.Duration
	MaxFilterLatency    time.Duration
	MinFilterLatency    time.Duration
	SumFilterLatency    time.Duration
	MaxRequestLatency   time.Duration
	MinRequestLatency   time.Duration
	SumRequestLatency   time.Duration
	MaxE2ELatency       time.Duration
	MinE2ELatency       time.Duration
	SumE2ELatency       time.Duration
}

// AppendWrite adds a normal TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWrite(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.TargetResults++
	b.MsgSent += res.SentCount
	b.MsgFailed += res.FailedCount
	b.MsgTotal += res.Total()

	b.appendWriteResult(res)
}

// AppendWriteOversized adds an oversized TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWriteOversized(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.OversizedTargetResults++
	b.OversizedMsgSent += res.SentCount
	b.OversizedMsgFailed += res.FailedCount
	b.OversizedMsgTotal += res.Total()

	b.appendWriteResult(res)
}

// AppendWriteInvalid adds an invalid TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWriteInvalid(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.InvalidTargetResults++
	b.InvalidMsgSent += res.SentCount
	b.InvalidMsgFailed += res.FailedCount
	b.InvalidMsgTotal += res.Total()

	b.appendWriteResult(res)
}

func (b *ObserverBuffer) appendWriteResult(res *TargetWriteResult) {
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

	if b.MaxTransformLatency < res.MaxTransformLatency {
		b.MaxTransformLatency = res.MaxTransformLatency
	}
	if b.MinTransformLatency > res.MinTransformLatency || b.MinTransformLatency == time.Duration(0) {
		b.MinTransformLatency = res.MinTransformLatency
	}
	b.SumTransformLatency += res.AvgTransformLatency

	if b.MaxRequestLatency < res.MaxRequestLatency {
		b.MaxRequestLatency = res.MaxRequestLatency
	}
	if b.MinRequestLatency > res.MinRequestLatency || b.MinRequestLatency == time.Duration(0) {
		b.MinRequestLatency = res.MinRequestLatency
	}
	b.SumRequestLatency += res.AvgRequestLatency

	if b.MaxE2ELatency < res.MaxE2ELatency {
		b.MaxE2ELatency = res.MaxE2ELatency
	}
	if b.MinE2ELatency > res.MinE2ELatency || b.MinE2ELatency == time.Duration(0) {
		b.MinE2ELatency = res.MinE2ELatency
	}
	b.SumE2ELatency += res.AvgE2ELatency
}

// AppendFiltered adds a FilterResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendFiltered(res *FilterResult) {
	if res == nil {
		return
	}

	b.MsgFiltered += res.FilteredCount
	b.appendFilterResult(res)
}

func (b *ObserverBuffer) appendFilterResult(res *FilterResult) {
	if b.MaxFilterLatency < res.MaxFilterLatency {
		b.MaxFilterLatency = res.MaxFilterLatency
	}
	if b.MinFilterLatency > res.MinFilterLatency || b.MinFilterLatency == time.Duration(0) {
		b.MinFilterLatency = res.MinFilterLatency
	}
	b.SumFilterLatency += res.AvgFilterLatency
}

// GetSumResults returns the total number of results logged in the buffer
func (b *ObserverBuffer) GetSumResults() int64 {
	return b.TargetResults + b.OversizedTargetResults + b.InvalidTargetResults
}

// GetAvgProcLatency calculates average processing latency
func (b *ObserverBuffer) GetAvgProcLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumProcLatency, b.GetSumResults())
}

// GetAvgMsgLatency calculates average message latency
func (b *ObserverBuffer) GetAvgMsgLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumMsgLatency, b.GetSumResults())
}

// GetAvgTransformLatency calculates average transformation latency
func (b *ObserverBuffer) GetAvgTransformLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumTransformLatency, b.MsgTotal)
}

// GetAvgFilterLatency calculates average filter latency
func (b *ObserverBuffer) GetAvgFilterLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumFilterLatency, b.MsgFiltered)
}

// GetAvgRequestLatency calculates average request latency
func (b *ObserverBuffer) GetAvgRequestLatency() time.Duration {
	return common.GetAverageFromDuration(b.SumRequestLatency, b.MsgTotal)
}

// GetAvgE2ELatency calculates average E2E latency
func (b *ObserverBuffer) GetAvgE2ELatency() time.Duration {
	return common.GetAverageFromDuration(b.SumE2ELatency, b.MsgTotal)
}

func (b *ObserverBuffer) String() string {
	return fmt.Sprintf(
		"TargetResults:%d,MsgFiltered:%d,MsgSent:%d,MsgFailed:%d,OversizedTargetResults:%d,OversizedMsgSent:%d,OversizedMsgFailed:%d,InvalidTargetResults:%d,InvalidMsgSent:%d,InvalidMsgFailed:%d,MaxProcLatency:%d,MaxMsgLatency:%d,MaxFilterLatency:%d,MaxTransformLatency:%d,SumTransformLatency:%d,SumProcLatency:%d,SumMsgLatency:%d,MinReqLatency:%d,MaxReqLatency:%d,SumReqLatency:%d,MinE2ELatency:%d,MaxE2ELatency:%d,SumE2ELatency:%d",
		b.TargetResults,
		b.MsgFiltered,
		b.MsgSent,
		b.MsgFailed,
		b.OversizedTargetResults,
		b.OversizedMsgSent,
		b.OversizedMsgFailed,
		b.InvalidTargetResults,
		b.InvalidMsgSent,
		b.InvalidMsgFailed,
		b.MaxProcLatency.Milliseconds(),
		b.MaxMsgLatency.Milliseconds(),
		b.MaxFilterLatency.Milliseconds(),
		b.MaxTransformLatency.Milliseconds(),
		b.SumTransformLatency.Milliseconds(), // Sums are reported to allow us to compute averages across multi-instance deployments
		b.SumProcLatency.Milliseconds(),
		b.SumMsgLatency.Milliseconds(),
		b.MinRequestLatency.Milliseconds(),
		b.MaxRequestLatency.Milliseconds(),
		b.SumRequestLatency.Milliseconds(),
		b.MinE2ELatency.Milliseconds(),
		b.MaxE2ELatency.Milliseconds(),
		b.SumE2ELatency.Milliseconds(),
	)
}
