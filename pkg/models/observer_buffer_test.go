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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestObserverBuffer(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{
		InvalidErrors: make(map[MetadataCodeDescription]int),
		FailedErrors:  make(map[MetadataCodeDescription]int),
	}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                      []byte("Baz"),
			PartitionKey:              "partition1",
			CollectorTstamp:           timeNow.Add(time.Duration(-60) * time.Minute),
			TimeCreated:               timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:                timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformationStarted: timeNow.Add(time.Duration(-3) * time.Minute),
			TimeTransformed:           timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestStarted:        timeNow.Add(time.Duration(-1) * time.Minute),
			TimeRequestFinished:       timeNow,
		},
		{
			Data:                      []byte("Bar"),
			PartitionKey:              "partition2",
			CollectorTstamp:           timeNow.Add(time.Duration(-80) * time.Minute),
			TimeCreated:               timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:                timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformationStarted: timeNow.Add(time.Duration(-6) * time.Minute),
			TimeTransformed:           timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestStarted:        timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestFinished:       timeNow,
		},
	}
	failed := []*Message{
		{
			Data:                      []byte("Foo"),
			PartitionKey:              "partition3",
			CollectorTstamp:           timeNow.Add(time.Duration(-40) * time.Minute),
			TimeCreated:               timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:                timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformationStarted: timeNow.Add(time.Duration(-9) * time.Minute),
			TimeTransformed:           timeNow.Add(time.Duration(-6) * time.Minute),
			TimeRequestStarted:        timeNow.Add(time.Duration(-5) * time.Minute),
			TimeRequestFinished:       timeNow,
		},
	}
	filtered := []*Message{
		{
			Data:                []byte("FooBar"),
			PartitionKey:        "partition4",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-9) * time.Minute),
			TimeRequestStarted:  timeNow.Add(time.Duration(-8) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}

	transformResult := &TransformationResult{
		Result: append(sent, failed...),
	}
	b.AppendTransformed(transformResult)

	r := NewTargetWriteResult(sent, failed, nil, nil)

	b.AppendWrite(r)
	b.AppendWrite(r)
	b.AppendWrite(nil)
	b.AppendWriteOversized(r)
	b.AppendWriteOversized(r)
	b.AppendWriteOversized(nil)
	b.AppendWriteInvalid(r)
	b.AppendWriteInvalid(r)
	b.AppendWriteInvalid(nil)

	fr := newFilterResultWithTime(filtered, timeNow)

	b.AppendFiltered(fr)

	assert.Equal(int64(2), b.TargetResults)
	assert.Equal(int64(4), b.MsgSent)
	assert.Equal(int64(2), b.MsgFailed)

	assert.Equal(int64(1), b.MsgFiltered)

	assert.Equal(int64(2), b.OversizedTargetResults)
	assert.Equal(int64(4), b.OversizedMsgSent)
	assert.Equal(int64(2), b.OversizedMsgFailed)

	assert.Equal(int64(2), b.InvalidTargetResults)
	assert.Equal(int64(4), b.InvalidMsgSent)
	assert.Equal(int64(2), b.InvalidMsgFailed)
	assert.Equal(int64(6), b.RequestCount)

	assert.Equal(time.Duration(7)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(70)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(3)*time.Minute, b.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinTransformLatency)

	assert.Equal(time.Duration(10)*time.Minute, b.MaxFilterLatency)
	assert.Equal(time.Duration(10)*time.Minute, b.MinFilterLatency)

	assert.Equal(time.Duration(5)*time.Minute, b.MaxRequestLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinRequestLatency)

	assert.Equal(time.Duration(80)*time.Minute, b.MaxE2ELatency)
	assert.Equal(time.Duration(60)*time.Minute, b.MinE2ELatency)

	assert.Equal("TargetResults:2,MsgFiltered:1,MsgSent:4,MsgFailed:2,RequestCount:6,OversizedTargetResults:2,OversizedMsgSent:4,OversizedMsgFailed:2,InvalidTargetResults:2,InvalidMsgSent:4,InvalidMsgFailed:2,MinProcLatency:240000,MaxProcLatency:420000,MinMsgLatency:3000000,MaxMsgLatency:4200000,SumMsgLatency:14400000,MinFilterLatency:600000,MaxFilterLatency:600000,MinTransformLatency:60000,MaxTransformLatency:180000,MinReqLatency:60000,MaxReqLatency:300000,SumReqLatency:960000,MinE2ELatency:3600000,MaxE2ELatency:4800000,SumE2ELatency:16800000", b.String())
}

// TestObserverBuffer_Basic is a basic version of the above test, stripping away all but one event
// It was created in order to provide a simpler way to investigate whether logging may be misreporting latency
func TestObserverBuffer_Basic(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{
		InvalidErrors: make(map[MetadataCodeDescription]int),
		FailedErrors:  make(map[MetadataCodeDescription]int),
	}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                      []byte("Baz"),
			PartitionKey:              "partition1",
			TimeCreated:               timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:                timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformationStarted: timeNow.Add(time.Duration(-3) * time.Minute),
			TimeTransformed:           timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestStarted:        timeNow.Add(time.Duration(-1) * time.Minute),
			TimeRequestFinished:       timeNow,
		},
	}
	transformResult := &TransformationResult{
		Result: sent,
	}
	b.AppendTransformed(transformResult)

	r := NewTargetWriteResult(sent, nil, nil, nil)

	b.AppendWrite(r)
	b.AppendWrite(nil)
	// b.AppendWriteOversized(r)
	b.AppendWriteOversized(nil)
	// b.AppendWriteInvalid(r)
	b.AppendWriteInvalid(nil)

	fr := newFilterResultWithTime(nil, timeNow)

	b.AppendFiltered(fr)

	assert.Equal(int64(1), b.TargetResults)
	assert.Equal(int64(1), b.MsgSent)
	assert.Equal(int64(0), b.MsgFailed)

	assert.Equal(int64(0), b.MsgFiltered)

	assert.Equal(int64(0), b.OversizedTargetResults)
	assert.Equal(int64(0), b.OversizedMsgSent)
	assert.Equal(int64(0), b.OversizedMsgFailed)

	assert.Equal(int64(0), b.InvalidTargetResults)
	assert.Equal(int64(0), b.InvalidMsgSent)
	assert.Equal(int64(0), b.InvalidMsgFailed)
	assert.Equal(int64(1), b.RequestCount)

	assert.Equal(time.Duration(4)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinTransformLatency)

	assert.Equal(time.Duration(0), b.MaxFilterLatency)
	assert.Equal(time.Duration(0), b.MinFilterLatency)

	assert.Equal(time.Duration(1)*time.Minute, b.MaxRequestLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinRequestLatency)

	assert.Equal("TargetResults:1,MsgFiltered:0,MsgSent:1,MsgFailed:0,RequestCount:1,OversizedTargetResults:0,OversizedMsgSent:0,OversizedMsgFailed:0,InvalidTargetResults:0,InvalidMsgSent:0,InvalidMsgFailed:0,MinProcLatency:240000,MaxProcLatency:240000,MinMsgLatency:3000000,MaxMsgLatency:3000000,SumMsgLatency:3000000,MinFilterLatency:0,MaxFilterLatency:0,MinTransformLatency:60000,MaxTransformLatency:60000,MinReqLatency:60000,MaxReqLatency:60000,SumReqLatency:60000,MinE2ELatency:0,MaxE2ELatency:0,SumE2ELatency:0", b.String())
}

// TestObserverBuffer_BasicNoTransform is a basic version of the above test, stripping away all but one event.
// It exists purely to simplify reasoning through bugs.
func TestObserverBuffer_BasicNoTransform(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{
		InvalidErrors: make(map[MetadataCodeDescription]int),
		FailedErrors:  make(map[MetadataCodeDescription]int),
	}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                []byte("Baz"),
			PartitionKey:        "partition1",
			TimeCreated:         timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestStarted:  timeNow.Add(time.Duration(-1) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}

	r := NewTargetWriteResult(sent, nil, nil, nil)

	b.AppendWrite(r)
	b.AppendWrite(nil)
	b.AppendWriteOversized(nil)
	b.AppendWriteInvalid(nil)

	fr := newFilterResultWithTime(nil, timeNow)

	b.AppendFiltered(fr)

	assert.Equal(int64(1), b.TargetResults)
	assert.Equal(int64(1), b.MsgSent)
	assert.Equal(int64(0), b.MsgFailed)

	assert.Equal(int64(0), b.MsgFiltered)

	assert.Equal(int64(0), b.OversizedTargetResults)
	assert.Equal(int64(0), b.OversizedMsgSent)
	assert.Equal(int64(0), b.OversizedMsgFailed)

	assert.Equal(int64(0), b.InvalidTargetResults)
	assert.Equal(int64(0), b.InvalidMsgSent)
	assert.Equal(int64(0), b.InvalidMsgFailed)
	assert.Equal(int64(1), b.RequestCount)

	assert.Equal(time.Duration(4)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(0), b.MaxTransformLatency)
	assert.Equal(time.Duration(0), b.MinTransformLatency)

	assert.Equal(time.Duration(0), b.MaxFilterLatency)
	assert.Equal(time.Duration(0), b.MinFilterLatency)

	assert.Equal(time.Duration(1)*time.Minute, b.MaxRequestLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinRequestLatency)

	assert.Equal("TargetResults:1,MsgFiltered:0,MsgSent:1,MsgFailed:0,RequestCount:1,OversizedTargetResults:0,OversizedMsgSent:0,OversizedMsgFailed:0,InvalidTargetResults:0,InvalidMsgSent:0,InvalidMsgFailed:0,MinProcLatency:240000,MaxProcLatency:240000,MinMsgLatency:3000000,MaxMsgLatency:3000000,SumMsgLatency:3000000,MinFilterLatency:0,MaxFilterLatency:0,MinTransformLatency:0,MaxTransformLatency:0,MinReqLatency:60000,MaxReqLatency:60000,SumReqLatency:60000,MinE2ELatency:0,MaxE2ELatency:0,SumE2ELatency:0", b.String())
}
