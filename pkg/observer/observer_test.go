//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package observer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

// --- Test StatsReceiver

type TestStatsReceiver struct {
	onSend func(b *models.ObserverBuffer)
}

func (s *TestStatsReceiver) Send(b *models.ObserverBuffer) {
	s.onSend(b)
}

// --- Tests

func TestObserverTargetWrite(t *testing.T) {
	assert := assert.New(t)

	counter := 0
	onSend := func(b *models.ObserverBuffer) {
		assert.NotNil(b)
		if counter == 0 {
			assert.Equal(int64(5), b.TargetResults)
			assert.Equal(int64(5), b.OversizedTargetResults)
			assert.Equal(int64(5), b.InvalidTargetResults)
			counter++
		} else {
			assert.Equal(int64(1), b.TargetResults)
			assert.Equal(int64(1), b.OversizedTargetResults)
			assert.Equal(int64(1), b.InvalidTargetResults)
		}
	}

	sr := TestStatsReceiver{onSend: onSend}

	observer := New(&sr, 1*time.Second, 3*time.Second)
	assert.NotNil(observer)
	observer.Start()

	// This does nothing
	observer.Start()

	// Push some results
	timeNow := time.Now().UTC()
	sent := []*models.Message{
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
	failed := []*models.Message{
		{
			Data:         []byte("Foo"),
			PartitionKey: "partition3",
			TimeCreated:  timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-10) * time.Minute),
		},
	}
	r := models.NewTargetWriteResultWithTime(sent, failed, nil, nil, timeNow)
	for i := 0; i < 5; i++ {
		observer.TargetWrite(r)
		observer.TargetWriteOversized(r)
		observer.TargetWriteInvalid(r)
	}

	// Trigger timeout (1 second)
	time.Sleep(2 * time.Second)

	// Trigger flush (3 seconds) - first counter check
	time.Sleep(2 * time.Second)

	// Trigger emergency flush (4 seconds) - second counter check
	observer.TargetWrite(r)
	observer.TargetWriteOversized(r)
	observer.TargetWriteInvalid(r)

	time.Sleep(1 * time.Second)

	observer.Stop()
}
