//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestObserverBuffer(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                []byte("Baz"),
			PartitionKey:        "partition1",
			TimeCreated:         timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestStarted:  timeNow.Add(time.Duration(-1) * time.Minute),
			TimeRequestFinished: timeNow,
		},
		{
			Data:                []byte("Bar"),
			PartitionKey:        "partition2",
			TimeCreated:         timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestStarted:  timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed := []*Message{
		{
			Data:                []byte("Foo"),
			PartitionKey:        "partition3",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-9) * time.Minute),
			TimeRequestStarted:  timeNow.Add(time.Duration(-8) * time.Minute),
			TimeRequestFinished: timeNow,
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

	r := NewTargetWriteResultWithTime(sent, failed, nil, nil, timeNow)

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
	assert.Equal(int64(6), b.MsgTotal)

	assert.Equal(int64(1), b.MsgFiltered)

	assert.Equal(int64(2), b.OversizedTargetResults)
	assert.Equal(int64(4), b.OversizedMsgSent)
	assert.Equal(int64(2), b.OversizedMsgFailed)
	assert.Equal(int64(6), b.OversizedMsgTotal)

	assert.Equal(int64(2), b.InvalidTargetResults)
	assert.Equal(int64(4), b.InvalidMsgSent)
	assert.Equal(int64(2), b.InvalidMsgFailed)
	assert.Equal(int64(6), b.InvalidMsgTotal)

	assert.Equal(time.Duration(10)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(7)*time.Minute, b.GetAvgProcLatency())
	assert.Equal(time.Duration(70)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(30)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.GetAvgMsgLatency())
	assert.Equal(time.Duration(3)*time.Minute, b.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinTransformLatency)
	assert.Equal(time.Duration(2)*time.Minute, b.GetAvgTransformLatency())

	assert.Equal(time.Duration(10)*time.Minute, b.MaxFilterLatency)
	assert.Equal(time.Duration(10)*time.Minute, b.MinFilterLatency)
	assert.Equal(time.Duration(10)*time.Minute, b.GetAvgFilterLatency())

	assert.Equal(time.Duration(8)*time.Minute, b.MaxRequestLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinRequestLatency)

	assert.Equal("TargetResults:2,MsgFiltered:1,MsgSent:4,MsgFailed:2,OversizedTargetResults:2,OversizedMsgSent:4,OversizedMsgFailed:2,InvalidTargetResults:2,InvalidMsgSent:4,InvalidMsgFailed:2,MaxProcLatency:600000,MaxMsgLatency:4200000,MaxFilterLatency:600000,MaxTransformLatency:180000,SumTransformLatency:720000,SumProcLatency:2520000,SumMsgLatency:18000000,MinReqLatency:60000,MaxReqLatency:480000,SumReqLatency:1320000", b.String())
}

// TestObserverBuffer_Basic is a basic version of the above test, stripping away all but one event
// It was created in order to provide a simpler way to investigate whether logging may be misreporting latency
func TestObserverBuffer_Basic(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:            []byte("Baz"),
			PartitionKey:    "partition1",
			TimeCreated:     timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-2) * time.Minute),
		},
	}

	r := NewTargetWriteResultWithTime(sent, nil, nil, nil, timeNow)

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
	assert.Equal(int64(1), b.MsgTotal)

	assert.Equal(int64(0), b.MsgFiltered)

	assert.Equal(int64(0), b.OversizedTargetResults)
	assert.Equal(int64(0), b.OversizedMsgSent)
	assert.Equal(int64(0), b.OversizedMsgFailed)
	assert.Equal(int64(0), b.OversizedMsgTotal)

	assert.Equal(int64(0), b.InvalidTargetResults)
	assert.Equal(int64(0), b.InvalidMsgSent)
	assert.Equal(int64(0), b.InvalidMsgFailed)
	assert.Equal(int64(0), b.InvalidMsgTotal)

	assert.Equal(time.Duration(4)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.GetAvgProcLatency())
	assert.Equal(time.Duration(50)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.GetAvgMsgLatency())
	assert.Equal(time.Duration(2)*time.Minute, b.MaxTransformLatency)
	assert.Equal(time.Duration(2)*time.Minute, b.MinTransformLatency)
	assert.Equal(time.Duration(2)*time.Minute, b.GetAvgTransformLatency())

	assert.Equal(time.Duration(0), b.MaxFilterLatency)
	assert.Equal(time.Duration(0), b.MinFilterLatency)
	assert.Equal(time.Duration(0), b.GetAvgFilterLatency())

	assert.Equal("TargetResults:1,MsgFiltered:0,MsgSent:1,MsgFailed:0,OversizedTargetResults:0,OversizedMsgSent:0,OversizedMsgFailed:0,InvalidTargetResults:0,InvalidMsgSent:0,InvalidMsgFailed:0,MaxProcLatency:240000,MaxMsgLatency:3000000,MaxFilterLatency:0,MaxTransformLatency:120000,SumTransformLatency:120000,SumProcLatency:240000,SumMsgLatency:3000000", b.String())
}

// TestObserverBuffer_Basic is a basic version of the above test, stripping away all but one event.
// It exists purely to simplify reasoning through bugs.
func TestObserverBuffer_BasicNoTransform(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:         []byte("Baz"),
			PartitionKey: "partition1",
			TimeCreated:  timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-4) * time.Minute),
		},
	}

	r := NewTargetWriteResultWithTime(sent, nil, nil, nil, timeNow)

	b.AppendWrite(r)
	b.AppendWrite(nil)
	b.AppendWriteOversized(nil)
	b.AppendWriteInvalid(nil)

	fr := newFilterResultWithTime(nil, timeNow)

	b.AppendFiltered(fr)

	assert.Equal(int64(1), b.TargetResults)
	assert.Equal(int64(1), b.MsgSent)
	assert.Equal(int64(0), b.MsgFailed)
	assert.Equal(int64(1), b.MsgTotal)

	assert.Equal(int64(0), b.MsgFiltered)

	assert.Equal(int64(0), b.OversizedTargetResults)
	assert.Equal(int64(0), b.OversizedMsgSent)
	assert.Equal(int64(0), b.OversizedMsgFailed)
	assert.Equal(int64(0), b.OversizedMsgTotal)

	assert.Equal(int64(0), b.InvalidTargetResults)
	assert.Equal(int64(0), b.InvalidMsgSent)
	assert.Equal(int64(0), b.InvalidMsgFailed)
	assert.Equal(int64(0), b.InvalidMsgTotal)

	assert.Equal(time.Duration(4)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.GetAvgProcLatency())
	assert.Equal(time.Duration(50)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.GetAvgMsgLatency())
	assert.Equal(time.Duration(0), b.MaxTransformLatency)
	assert.Equal(time.Duration(0), b.MinTransformLatency)
	assert.Equal(time.Duration(0), b.GetAvgTransformLatency())

	assert.Equal(time.Duration(0), b.MaxFilterLatency)
	assert.Equal(time.Duration(0), b.MinFilterLatency)
	assert.Equal(time.Duration(0), b.GetAvgFilterLatency())

	assert.Equal("TargetResults:1,MsgFiltered:0,MsgSent:1,MsgFailed:0,OversizedTargetResults:0,OversizedMsgSent:0,OversizedMsgFailed:0,InvalidTargetResults:0,InvalidMsgSent:0,InvalidMsgFailed:0,MaxProcLatency:240000,MaxMsgLatency:3000000,MaxFilterLatency:0,MaxTransformLatency:0,SumTransformLatency:0,SumProcLatency:240000,SumMsgLatency:3000000", b.String())
}
