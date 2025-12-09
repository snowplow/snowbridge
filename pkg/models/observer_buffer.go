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
)

// MetadataEvent holds data required for metadata reporter's event
type MetadataCodeDescription struct {
	Code        string
	Description string
}

// ObserverBuffer contains all the metrics we are processing
type ObserverBuffer struct {
	TargetResults int64
	MsgSent       int64
	MsgFailed     int64
	MsgFiltered   int64
	RequestCount  int64

	OversizedTargetResults int64
	OversizedMsgSent       int64
	OversizedMsgFailed     int64

	InvalidTargetResults int64
	InvalidMsgSent       int64
	InvalidMsgFailed     int64

	MaxProcLatency      time.Duration
	MinProcLatency      time.Duration
	MaxMsgLatency       time.Duration
	MinMsgLatency       time.Duration
	SumMsgLatency       time.Duration
	MaxTransformLatency time.Duration
	MinTransformLatency time.Duration
	MaxFilterLatency    time.Duration
	MinFilterLatency    time.Duration
	MaxRequestLatency   time.Duration
	MinRequestLatency   time.Duration
	SumRequestLatency   time.Duration
	MaxE2ELatency       time.Duration
	MinE2ELatency       time.Duration
	SumE2ELatency       time.Duration

	InvalidErrors map[MetadataCodeDescription]int
	FailedErrors  map[MetadataCodeDescription]int

	// Kinsumer metrics
	KinsumerRecordsInMemory      int64 // Current count of records in memory
	KinsumerRecordsInMemoryBytes int64 // Current bytes of records in memory
}

func (b *ObserverBuffer) appendInvalidError(msgs []*Message) {
	for _, msg := range msgs {
		if sem, ok := msg.GetError().(SanitisedErrorMetadata); ok {
			e := MetadataCodeDescription{
				Code:        sem.Code(),
				Description: sem.SanitisedError(),
			}
			b.InvalidErrors[e] = b.InvalidErrors[e] + 1
		}
	}
}

func (b *ObserverBuffer) appendFailedError(msgs []*Message) {
	for _, msg := range msgs {
		if sem, ok := msg.GetError().(SanitisedErrorMetadata); ok {
			e := MetadataCodeDescription{
				Code:        sem.Code(),
				Description: sem.SanitisedError(),
			}
			b.FailedErrors[e] = b.FailedErrors[e] + 1
		}
	}
}

// AppendWrite adds a normal TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWrite(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.TargetResults++
	b.MsgSent += int64(len(res.Sent))
	b.MsgFailed += int64(len(res.Failed))

	// Appending errors metadata
	b.appendFailedError(res.Failed)

	// Calculate processing, message, and E2E latencies only for successfully sent messages
	for _, msg := range res.Sent {
		procLatency := msg.TimeRequestFinished.Sub(msg.TimePulled)
		if b.MaxProcLatency < procLatency {
			b.MaxProcLatency = procLatency
		}
		if b.MinProcLatency > procLatency || b.MinProcLatency == time.Duration(0) {
			b.MinProcLatency = procLatency
		}

		messageLatency := msg.TimeRequestFinished.Sub(msg.TimeCreated)
		b.SumMsgLatency += messageLatency
		if b.MaxMsgLatency < messageLatency {
			b.MaxMsgLatency = messageLatency
		}
		if b.MinMsgLatency > messageLatency || b.MinMsgLatency == time.Duration(0) {
			b.MinMsgLatency = messageLatency
		}

		if !msg.CollectorTstamp.IsZero() {
			e2eLatency := msg.TimeRequestFinished.Sub(msg.CollectorTstamp)
			b.SumE2ELatency += e2eLatency
			if b.MaxE2ELatency < e2eLatency {
				b.MaxE2ELatency = e2eLatency
			}
			if b.MinE2ELatency > e2eLatency || b.MinE2ELatency == time.Duration(0) {
				b.MinE2ELatency = e2eLatency
			}
		}
	}

	// Calculate request latency for all messages (sent, failed, invalid)
	allMessages := append(append(res.Sent, res.Failed...), res.Invalid...)
	for _, msg := range allMessages {
		if !msg.TimeRequestStarted.IsZero() && !msg.TimeRequestFinished.IsZero() {
			b.RequestCount++

			requestLatency := msg.TimeRequestFinished.Sub(msg.TimeRequestStarted)
			b.SumRequestLatency += requestLatency

			if b.MaxRequestLatency < requestLatency {
				b.MaxRequestLatency = requestLatency
			}
			if b.MinRequestLatency > requestLatency || b.MinRequestLatency == time.Duration(0) {
				b.MinRequestLatency = requestLatency
			}
		}
	}
}

// AppendWriteOversized adds an oversized TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWriteOversized(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.OversizedTargetResults++
	b.OversizedMsgSent += int64(len(res.Sent))
	b.OversizedMsgFailed += int64(len(res.Failed))
}

// AppendWriteInvalid adds an invalid TargetWriteResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendWriteInvalid(res *TargetWriteResult) {
	if res == nil {
		return
	}

	b.InvalidTargetResults++
	b.InvalidMsgSent += int64(len(res.Sent))
	b.InvalidMsgFailed += int64(len(res.Failed))

	// Appending errors metadata
	b.appendInvalidError(res.Sent)
}

// AppendFiltered adds a FilterResult onto the buffer and stores the result
func (b *ObserverBuffer) AppendFiltered(res *FilterResult) {
	if res == nil {
		return
	}

	b.MsgFiltered += res.FilteredCount
	if b.MaxFilterLatency < res.MaxFilterLatency {
		b.MaxFilterLatency = res.MaxFilterLatency
	}
	if b.MinFilterLatency > res.MinFilterLatency || b.MinFilterLatency == time.Duration(0) {
		b.MinFilterLatency = res.MinFilterLatency
	}
}

func (b *ObserverBuffer) AppendTransformed(res *TransformationResult) {
	if res == nil {
		return
	}

	for _, msg := range res.Result {
		if !msg.TimeTransformationStarted.IsZero() && !msg.TimeTransformed.IsZero() {
			transformLatency := msg.TimeTransformed.Sub(msg.TimeTransformationStarted)
			if b.MaxTransformLatency < transformLatency {
				b.MaxTransformLatency = transformLatency
			}
			if b.MinTransformLatency > transformLatency || b.MinTransformLatency == time.Duration(0) {
				b.MinTransformLatency = transformLatency
			}
		}
	}
}

func (b *ObserverBuffer) String() string {
	return fmt.Sprintf(
		"TargetResults:%d,MsgFiltered:%d,MsgSent:%d,MsgFailed:%d,RequestCount:%d,OversizedTargetResults:%d,OversizedMsgSent:%d,OversizedMsgFailed:%d,InvalidTargetResults:%d,InvalidMsgSent:%d,InvalidMsgFailed:%d,MinProcLatency:%d,MaxProcLatency:%d,MinMsgLatency:%d,MaxMsgLatency:%d,SumMsgLatency:%d,MinFilterLatency:%d,MaxFilterLatency:%d,MinTransformLatency:%d,MaxTransformLatency:%d,MinReqLatency:%d,MaxReqLatency:%d,SumReqLatency:%d,MinE2ELatency:%d,MaxE2ELatency:%d,SumE2ELatency:%d",
		b.TargetResults,
		b.MsgFiltered,
		b.MsgSent,
		b.MsgFailed,
		b.RequestCount,
		b.OversizedTargetResults,
		b.OversizedMsgSent,
		b.OversizedMsgFailed,
		b.InvalidTargetResults,
		b.InvalidMsgSent,
		b.InvalidMsgFailed,
		b.MinProcLatency.Milliseconds(),
		b.MaxProcLatency.Milliseconds(),
		b.MinMsgLatency.Milliseconds(),
		b.MaxMsgLatency.Milliseconds(),
		b.SumMsgLatency.Milliseconds(),
		b.MinFilterLatency.Milliseconds(),
		b.MaxFilterLatency.Milliseconds(),
		b.MinTransformLatency.Milliseconds(),
		b.MaxTransformLatency.Milliseconds(),
		b.MinRequestLatency.Milliseconds(),
		b.MaxRequestLatency.Milliseconds(),
		b.SumRequestLatency.Milliseconds(),
		b.MinE2ELatency.Milliseconds(),
		b.MaxE2ELatency.Milliseconds(),
		b.SumE2ELatency.Milliseconds(),
	)
}
