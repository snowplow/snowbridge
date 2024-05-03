/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewTargetWriteResult_EmptyWithoutTime tests that an empty targetWriteResult with no timings will report 0s across the board
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

// TestNewTargetWriteResult_EmptyWithTime tests that an empty targetWriteResult with no a provided timestamp will report 0s across the board
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

// TestNewTargetWriteResult_WithMessages tests that reporting of statistics is as it should be when we have all data
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
	assert.Equal(time.Duration(3)*time.Minute, r.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, r.MinTransformLatency)
	assert.Equal(time.Duration(2)*time.Minute, r.AvgTransformLatency)

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
			TimeTransformed: timeNow.Add(time.Duration(-4) * time.Minute),
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
	assert.Equal(time.Duration(8)*time.Minute, r3.MaxTransformLatency)
	assert.Equal(time.Duration(1)*time.Minute, r3.MinTransformLatency)
	assert.Equal(time.Duration(3)*time.Minute, r3.AvgTransformLatency)
}

// TestNewTargetWriteResult_NoTransformation tests that reporting of statistics is as it should be when we don't have a timeTransformed
func TestNewTargetWriteResult_NoTransformation(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now().UTC()

	sent := []*Message{
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
	}
	failed := []*Message{
		{
			Data:         []byte("Foo"),
			PartitionKey: "partition3",
			TimeCreated:  timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-10) * time.Minute),
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
	assert.Equal(time.Duration(0), r.MaxTransformLatency)
	assert.Equal(time.Duration(0), r.MinTransformLatency)
	assert.Equal(time.Duration(0), r.AvgTransformLatency)
}
