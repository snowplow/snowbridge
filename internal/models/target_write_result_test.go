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

func TestNewTargetWriteResult_EmptyWithoutTime(t *testing.T) {
	assert := assert.New(t)

	r := NewTargetWriteResult(0, 0, nil, nil)
	assert.NotNil(r)

	assert.Equal(int64(0), r.Sent)
	assert.Equal(int64(0), r.Failed)
	assert.Equal(int64(0), r.Total())

	assert.Equal(time.Duration(0), r.MaxProcLatency)
	assert.Equal(time.Duration(0), r.MinProcLatency)
	assert.Equal(time.Duration(0), r.AvgProcLatency)

	assert.Equal(time.Duration(0), r.MaxMsgLatency)
	assert.Equal(time.Duration(0), r.MinMsgLatency)
	assert.Equal(time.Duration(0), r.AvgMsgLatency)
}

func TestNewTargetWriteResult_EmptyWithTime(t *testing.T) {
	assert := assert.New(t)

	r := NewTargetWriteResultWithTime(0, 0, time.Now().UTC(), nil, nil)
	assert.NotNil(r)

	assert.Equal(int64(0), r.Sent)
	assert.Equal(int64(0), r.Failed)
	assert.Equal(int64(0), r.Total())

	assert.Equal(time.Duration(0), r.MaxProcLatency)
	assert.Equal(time.Duration(0), r.MinProcLatency)
	assert.Equal(time.Duration(0), r.AvgProcLatency)

	assert.Equal(time.Duration(0), r.MaxMsgLatency)
	assert.Equal(time.Duration(0), r.MinMsgLatency)
	assert.Equal(time.Duration(0), r.AvgMsgLatency)
}

func TestNewTargetWriteResult_WithMessages(t *testing.T) {
	assert := assert.New(t)

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

	r := NewTargetWriteResultWithTime(2, 1, timeNow, messages, nil)
	assert.NotNil(r)

	assert.Equal(int64(2), r.Sent)
	assert.Equal(int64(1), r.Failed)
	assert.Equal(int64(3), r.Total())
	assert.Equal(time.Duration(10)*time.Minute, r.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, r.MinProcLatency)
	assert.Equal(time.Duration(7)*time.Minute, r.AvgProcLatency)
	assert.Equal(time.Duration(70)*time.Minute, r.MaxMsgLatency)
	assert.Equal(time.Duration(30)*time.Minute, r.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, r.AvgMsgLatency)

	messages1 := []*Message{
		{
			Data:         []byte("Baz"),
			PartitionKey: "partition1",
			TimeCreated:  timeNow.Add(time.Duration(-55) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-2) * time.Minute),
		},
		{
			Data:         []byte("Bar"),
			PartitionKey: "partition2",
			TimeCreated:  timeNow.Add(time.Duration(-75) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-7) * time.Minute),
		},
		{
			Data:         []byte("Foo"),
			PartitionKey: "partition3",
			TimeCreated:  timeNow.Add(time.Duration(-25) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-15) * time.Minute),
		},
	}

	r1 := NewTargetWriteResultWithTime(1, 2, timeNow, messages1, nil)
	assert.NotNil(r)

	// Append a result
	r2 := r.Append(r1)
	// Will not append anything
	r3 := r2.Append(nil)

	// Check that the result has not been mutated
	assert.Equal(int64(2), r.Sent)
	assert.Equal(int64(1), r.Failed)
	assert.Equal(int64(3), r.Total())

	// Check appended result
	assert.Equal(int64(3), r3.Sent)
	assert.Equal(int64(3), r3.Failed)
	assert.Equal(int64(6), r3.Total())
	assert.Equal(time.Duration(15)*time.Minute, r3.MaxProcLatency)
	assert.Equal(time.Duration(2)*time.Minute, r3.MinProcLatency)
	assert.Equal(time.Duration(450)*time.Second, r3.AvgProcLatency)
	assert.Equal(time.Duration(75)*time.Minute, r3.MaxMsgLatency)
	assert.Equal(time.Duration(25)*time.Minute, r3.MinMsgLatency)
	assert.Equal(time.Duration(3050)*time.Second, r3.AvgMsgLatency)
}
