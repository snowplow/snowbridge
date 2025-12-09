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

// TestNewTargetWriteResult_EmptyWithoutTime tests that an empty targetWriteResult with no timings will report 0s across the board
func TestNewTargetWriteResult_EmptyWithoutTime(t *testing.T) {
	assert := assert.New(t)

	r := NewTargetWriteResult(nil, nil, nil, nil)
	assert.NotNil(r)

	assert.Equal(0, len(r.Sent))
	assert.Equal(0, len(r.Failed))
}

// TestNewTargetWriteResult_WithMessages tests that reporting of statistics is as it should be when we have all data
func TestNewTargetWriteResult_WithMessages(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                []byte("Baz"),
			PartitionKey:        "partition1",
			CollectorTstamp:     timeNow.Add(time.Duration(-60) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-2) * time.Minute),
			TimeRequestFinished: timeNow,
		},
		{
			Data:                []byte("Bar"),
			PartitionKey:        "partition2",
			CollectorTstamp:     timeNow.Add(time.Duration(-80) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed := []*Message{
		{
			Data:                []byte("Foo"),
			PartitionKey:        "partition3",
			CollectorTstamp:     timeNow.Add(time.Duration(-40) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-9) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}

	r := NewTargetWriteResult(sent, failed, nil, nil)
	assert.NotNil(r)

	assert.Equal(2, len(r.Sent))
	assert.Equal(1, len(r.Failed))

	sent1 := []*Message{
		{
			Data:                []byte("Baz"),
			PartitionKey:        "partition1",
			CollectorTstamp:     timeNow.Add(time.Duration(-60) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-55) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-2) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-1) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed1 := []*Message{
		{
			Data:                []byte("Bar"),
			PartitionKey:        "partition2",
			CollectorTstamp:     timeNow.Add(time.Duration(-120) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-75) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-7) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestFinished: timeNow,
		},
		{
			Data:                []byte("Foo"),
			PartitionKey:        "partition3",
			CollectorTstamp:     timeNow.Add(time.Duration(-30) * time.Minute),
			TimeCreated:         timeNow.Add(time.Duration(-25) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-15) * time.Minute),
			TimeTransformed:     timeNow.Add(time.Duration(-7) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}

	r1 := NewTargetWriteResult(sent1, failed1, nil, nil)
	assert.NotNil(r)

	// Append a result
	r2 := r.Append(r1)
	// Will not append anything
	r3 := r2.Append(nil)

	// Check that the result has not been mutated
	assert.Equal(2, len(r.Sent))
	assert.Equal(1, len(r.Failed))

	// Check appended result
	assert.Equal(3, len(r3.Sent))
	assert.Equal(3, len(r3.Failed))
}

// TestNewTargetWriteResult_NoTransformation_NoE2E tests that reporting of statistics is as it should be when we don't have a timeTransformed and no collector timestamp
func TestNewTargetWriteResult_NoTransformation_NoE2E(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now().UTC()

	sent := []*Message{
		{
			Data:                []byte("Baz"),
			PartitionKey:        "partition1",
			TimeCreated:         timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-4) * time.Minute),
			TimeRequestFinished: timeNow,
		},
		{
			Data:                []byte("Bar"),
			PartitionKey:        "partition2",
			TimeCreated:         timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-7) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed := []*Message{
		{
			Data:                []byte("Foo"),
			PartitionKey:        "partition3",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}

	r := NewTargetWriteResult(sent, failed, nil, nil)
	assert.NotNil(r)

	assert.Equal(2, len(r.Sent))
	assert.Equal(1, len(r.Failed))
}
