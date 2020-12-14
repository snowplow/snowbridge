// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestObserverBuffer(t *testing.T) {
	assert := assert.New(t)

	b := ObserverBuffer{}
	assert.NotNil(b)

	timeNow := time.Now().UTC()

	messages := []*Message{
		{
			Data:         []byte("Baz"),
			PartitionKey: "partition1",
			TimeCreated:  timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-4) * time.Minute),
		},
		{
			Data:         []byte("Bar"),
			PartitionKey: "partition2",
			TimeCreated:  timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-7) * time.Minute),
		},
		{
			Data:         []byte("Foo"),
			PartitionKey: "partition3",
			TimeCreated:  timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-10) * time.Minute),
		},
	}

	r := NewWriteResultWithTime(2, 1, timeNow, messages)

	b.Append(r)
	b.Append(r)
	b.Append(nil)

	assert.Equal(int64(2), b.TargetResults)
	assert.Equal(int64(4), b.MsgSent)
	assert.Equal(int64(2), b.MsgFailed)
	assert.Equal(int64(6), b.MsgTotal)
	assert.Equal(time.Duration(10)*time.Minute, b.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, b.MinProcLatency)
	assert.Equal(time.Duration(7)*time.Minute, b.GetAvgProcLatency())
	assert.Equal(time.Duration(70)*time.Minute, b.MaxMsgLatency)
	assert.Equal(time.Duration(30)*time.Minute, b.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, b.GetAvgMsgLatency())
	assert.Equal("TargetResults:2,MsgSent:4,MsgFailed:2,MsgTotal:6,MaxProcLatency:10m0s,MinProcLatency:4m0s,AvgProcLatency:7m0s,MaxMsgLatency:1h10m0s,MinMsgLatency:30m0s,AvgMsgLatency:50m0s", b.String())
}
