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
	failed := []*models.Message{
		{
			Data:                []byte("Foo"),
			PartitionKey:        "partition3",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	r := models.NewTargetWriteResult(sent, failed, nil, nil)
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
