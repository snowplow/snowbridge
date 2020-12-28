// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewWriteResult_Empty(t *testing.T) {
	assert := assert.New(t)

	r := NewWriteResultWithTime(0, 0, time.Now().UTC(), nil)
	assert.NotNil(r)

	assert.Equal(int64(0), r.Sent)
	assert.Equal(int64(0), r.Failed)
	assert.Equal(int64(0), r.Total())

	assert.Equal(time.Duration(0), r.MaxProcLatency)
	assert.Equal(time.Duration(0), r.MinProcLatency)
	assert.Equal(time.Duration(0), r.AvgProcLatency)

	assert.Equal(time.Duration(0), r.MaxMessageLatency)
	assert.Equal(time.Duration(0), r.MinMessageLatency)
	assert.Equal(time.Duration(0), r.AvgMessageLatency)
}

func TestNewWriteResult_WithMessages(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now().UTC()

	messages := []*Event{
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
	assert.NotNil(r)

	assert.Equal(int64(2), r.Sent)
	assert.Equal(int64(1), r.Failed)
	assert.Equal(int64(3), r.Total())

	assert.Equal(time.Duration(10)*time.Minute, r.MaxProcLatency)
	assert.Equal(time.Duration(4)*time.Minute, r.MinProcLatency)
	assert.Equal(time.Duration(7)*time.Minute, r.AvgProcLatency)

	assert.Equal(time.Duration(70)*time.Minute, r.MaxMessageLatency)
	assert.Equal(time.Duration(30)*time.Minute, r.MinMessageLatency)
	assert.Equal(time.Duration(50)*time.Minute, r.AvgMessageLatency)
}
