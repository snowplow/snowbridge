// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTargetWriteResult_EmptyWithoutTime(t *testing.T) {
	assert := assert.New(t)

	r := NewTargetWriteResult(nil, nil, nil, nil)
	assert.NotNil(r)

	assert.Equal(int64(0), r.SentCount)
	assert.Equal(int64(0), r.FailedCount)
	assert.Equal(int64(0), r.Total())

	assert.Equal(time.Duration(0), r.MaxProcLatency)
	assert.Equal(time.Duration(0), r.MinProcLatency)
	assert.Equal(time.Duration(0), r.AvgProcLatency)

	assert.Equal(time.Duration(0), r.MaxMsgLatency)
	assert.Equal(time.Duration(0), r.MinMsgLatency)
	assert.Equal(time.Duration(0), r.AvgMsgLatency)

	assert.Equal(time.Duration(0), r.MaxTransformLatency)
	assert.Equal(time.Duration(0), r.MinTransformLatency)
	assert.Equal(time.Duration(0), r.AvgTransformLatency)
}

func TestNewTargetWriteResult_EmptyWithTime(t *testing.T) {
	assert := assert.New(t)

	r := NewTargetWriteResultWithTime(nil, nil, nil, nil, time.Now().UTC())
	assert.NotNil(r)

	assert.Equal(int64(0), r.SentCount)
	assert.Equal(int64(0), r.FailedCount)
	assert.Equal(int64(0), r.Total())

	assert.Equal(time.Duration(0), r.MaxProcLatency)
	assert.Equal(time.Duration(0), r.MinProcLatency)
	assert.Equal(time.Duration(0), r.AvgProcLatency)

	assert.Equal(time.Duration(0), r.MaxMsgLatency)
	assert.Equal(time.Duration(0), r.MinMsgLatency)
	assert.Equal(time.Duration(0), r.AvgMsgLatency)

	assert.Equal(time.Duration(0), r.MaxTransformLatency)
	assert.Equal(time.Duration(0), r.MinTransformLatency)
	assert.Equal(time.Duration(0), r.AvgTransformLatency)
}

func TestNewTargetWriteResult_WithMessages(t *testing.T) {
	assert := assert.New(t)

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

	r := NewTargetWriteResultWithTime(sent, failed, nil, nil, timeNow)
	assert.NotNil(r)

	assert.Equal(int64(2), r.SentCount)
	assert.Equal(int64(1), r.FailedCount)
	assert.Equal(int64(3), r.Total())
	assert.Equal(time.Duration(10)*time.Minute, r.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, r.MinProcLatency)
	assert.Equal(time.Duration(7)*time.Minute, r.AvgProcLatency)
	assert.Equal(time.Duration(70)*time.Minute, r.MaxMsgLatency)
	assert.Equal(time.Duration(30)*time.Minute, r.MinMsgLatency)
	assert.Equal(time.Duration(50)*time.Minute, r.AvgMsgLatency)
	assert.Equal(time.Duration(66)*time.Minute, r.MaxTransformLatency)
	assert.Equal(time.Duration(21)*time.Minute, r.MinTransformLatency)
	assert.Equal(time.Duration(45)*time.Minute, r.AvgTransformLatency)

	sent1 := []*Message{
		{
			Data:            []byte("Baz"),
			PartitionKey:    "partition1",
			TimeCreated:     timeNow.Add(time.Duration(-55) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-2) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-1) * time.Minute),
		},
	}
	failed1 := []*Message{
		{
			Data:            []byte("Bar"),
			PartitionKey:    "partition2",
			TimeCreated:     timeNow.Add(time.Duration(-75) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-6) * time.Minute),
		},
		{
			Data:            []byte("Foo"),
			PartitionKey:    "partition3",
			TimeCreated:     timeNow.Add(time.Duration(-25) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-15) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-7) * time.Minute),
		},
	}

	r1 := NewTargetWriteResultWithTime(sent1, failed1, nil, nil, timeNow)
	assert.NotNil(r)

	// Append a result
	r2 := r.Append(r1)
	// Will not append anything
	r3 := r2.Append(nil)

	// Check that the result has not been mutated
	assert.Equal(int64(2), r.SentCount)
	assert.Equal(int64(1), r.FailedCount)
	assert.Equal(int64(3), r.Total())

	// Check appended result
	assert.Equal(int64(3), r3.SentCount)
	assert.Equal(int64(3), r3.FailedCount)
	assert.Equal(int64(6), r3.Total())
	assert.Equal(time.Duration(15)*time.Minute, r3.MaxProcLatency)
	assert.Equal(time.Duration(2)*time.Minute, r3.MinProcLatency)
	assert.Equal(time.Duration(450)*time.Second, r3.AvgProcLatency)
	assert.Equal(time.Duration(75)*time.Minute, r3.MaxMsgLatency)
	assert.Equal(time.Duration(25)*time.Minute, r3.MinMsgLatency)
	assert.Equal(time.Duration(3050)*time.Second, r3.AvgMsgLatency)
	assert.Equal(time.Duration(69)*time.Minute, r3.MaxTransformLatency)
	assert.Equal(time.Duration(18)*time.Minute, r3.MinTransformLatency)
	assert.Equal(time.Duration(46)*time.Minute, r3.AvgTransformLatency)
}
