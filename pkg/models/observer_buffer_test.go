// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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
			Data:            []byte("Baz"),
			PartitionKey:    "partition1",
			TimeCreated:     timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-2) * time.Minute),
		},
		{
			Data:            []byte("Bar"),
			PartitionKey:    "partition2",
			TimeCreated:     timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-4) * time.Minute),
		},
	}
	failed := []*Message{
		{
			Data:            []byte("Foo"),
			PartitionKey:    "partition3",
			TimeCreated:     timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-9) * time.Minute),
		},
	}
	filtered := []*Message{
		{
			Data:            []byte("FooBar"),
			PartitionKey:    "partition4",
			TimeCreated:     timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-9) * time.Minute),
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
	assert.Equal(time.Duration(7)*time.Minute, b.getAvgProcLatency())
	assert.Equal(time.Duration(70)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(30)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.getAvgMsgLatency())
	assert.Equal(time.Duration(3)*time.Minute, b.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, b.MinTransformLatency)
	assert.Equal(time.Duration(2)*time.Minute, b.getAvgTransformLatency())

	assert.Equal(time.Duration(10)*time.Minute, b.MaxFilterLatency)
	assert.Equal(time.Duration(10)*time.Minute, b.MinFilterLatency)
	assert.Equal(time.Duration(10)*time.Minute, b.getAvgFilterLatency())

	assert.Equal("TargetResults:2,MsgFiltered:1,MsgSent:4,MsgFailed:2,OversizedTargetResults:2,OversizedMsgSent:4,OversizedMsgFailed:2,InvalidTargetResults:2,InvalidMsgSent:4,InvalidMsgFailed:2,MaxProcLatency:600000,MaxMsgLatency:4200000,MaxFilterLatency:600000,MaxTransformLatency:180000,SumTransformLatency:720000,SumProcLatency:2520000,SumMsgLatency:18000000", b.String())
}
