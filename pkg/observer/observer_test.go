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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v5/pkg/models"
)

// --- Test StatsReceiver

type TestStatsReceiver struct {
	onSend func(b *models.ObserverBuffer)
}

func (s *TestStatsReceiver) Send(b *models.ObserverBuffer) {
	s.onSend(b)
}

// --- Test MetadataReporter

type TestMetadataReporter struct {
	onSend func(b *models.ObserverBuffer)
}

func (s *TestMetadataReporter) Send(b *models.ObserverBuffer, _, _ time.Time) {
	s.onSend(b)
}

// --- Tests

func TestObserverTargetWrite(t *testing.T) {
	assert := assert.New(t)

	failedTempError := models.TemplatingError{
		SafeMessage: "failed safe message",
		Err:         fmt.Errorf("actual bad error"),
	}

	invalidTempError := models.TemplatingError{
		SafeMessage: "invalid safe message",
		Err:         fmt.Errorf("actual bad error"),
	}

	counter := 0
	onSend := func(b *models.ObserverBuffer) {
		assert.NotNil(b)
		if counter == 0 {
			assert.Equal(int64(5), b.TargetResults)
			assert.Equal(int64(5), b.InvalidTargetResults)
			counter++
		} else {
			assert.Equal(int64(1), b.TargetResults)
			assert.Equal(int64(1), b.InvalidTargetResults)
		}
	}

	metaCounter := 0
	onSendMetadata := func(b *models.ObserverBuffer) {
		assert.NotNil(b)
		if metaCounter == 0 {
			assert.Equal(int64(5), b.TargetResults)
			assert.Equal(int64(5), b.InvalidTargetResults)

			assert.Equal(1, len(b.FailedErrors))
			for kErr, v := range b.FailedErrors {
				assert.Equal(failedTempError.SafeMessage, kErr.Description)
				assert.Equal(5, v)
			}

			assert.Equal(1, len(b.InvalidErrors))
			for kErr, v := range b.InvalidErrors {
				assert.Equal(invalidTempError.SafeMessage, kErr.Description)
				assert.Equal(5, v)
			}

			metaCounter++
		} else {
			assert.Equal(int64(1), b.TargetResults)
			assert.Equal(int64(1), b.InvalidTargetResults)

			assert.Equal(1, len(b.FailedErrors))
			for kErr, v := range b.FailedErrors {
				assert.Equal(failedTempError.SafeMessage, kErr.Description)
				assert.Equal(1, v)
			}

			assert.Equal(1, len(b.InvalidErrors))
			for kErr, v := range b.InvalidErrors {
				assert.Equal(invalidTempError.SafeMessage, kErr.Description)
				assert.Equal(1, v)
			}
		}
	}

	sr := &TestStatsReceiver{onSend: onSend}
	mr := &TestMetadataReporter{onSend: onSendMetadata}

	observer := New(sr, 3*time.Second, mr)
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
		{
			Data:                []byte("InvalidFoo"),
			PartitionKey:        "partition4",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed := []*models.Message{
		{
			Data:                []byte("FailedFoo"),
			PartitionKey:        "partition3",
			TimeCreated:         timeNow.Add(time.Duration(-30) * time.Minute),
			TimePulled:          timeNow.Add(time.Duration(-10) * time.Minute),
			TimeRequestFinished: timeNow,
		},
	}
	failed[0].SetError(&failedTempError)
	sent[2].SetError(&invalidTempError)

	r := models.NewTargetWriteResult(sent, failed, nil)
	for range 5 {
		observer.TargetWrite(r)
		observer.TargetWriteInvalid(r)
	}

	// Trigger timeout (1 second)
	time.Sleep(2 * time.Second)

	// Trigger flush (3 seconds) - first counter check
	time.Sleep(2 * time.Second)

	// Trigger emergency flush (4 seconds) - second counter check
	observer.TargetWrite(r)
	observer.TargetWriteInvalid(r)

	time.Sleep(1 * time.Second)

	observer.Stop()
}

func TestObserverContinuesWhenMetadataHangs(t *testing.T) {
	a := assert.New(t)

	metaBlock := make(chan struct{})

	var statsCalls atomic.Int64
	sr := &TestStatsReceiver{onSend: func(b *models.ObserverBuffer) {
		statsCalls.Add(1)
	}}
	mr := &TestMetadataReporter{onSend: func(b *models.ObserverBuffer) {
		<-metaBlock
	}}

	observer := New(sr, 50*time.Millisecond, mr)
	observer.Start()

	// Let several ticks fire so the metadata Send is wedged and
	// the snapshot channel is filled.
	time.Sleep(250 * time.Millisecond)

	r := models.NewTargetWriteResult(nil, nil, nil)

	// Producers must not block on a wedged metadata endpoint.
	producerDone := make(chan struct{})
	go func() {
		for range 5000 {
			observer.TargetWrite(r)
			observer.TargetWriteInvalid(r)
			observer.TargetWriteFiltered(r)
		}
		close(producerDone)
	}()

	select {
	case <-producerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("producer blocked while metadata reporter was hanging")
	}

	// Statsd Send fires on every tick regardless of metadata wedge.
	a.GreaterOrEqual(statsCalls.Load(), int64(3), "statsd Send should fire on each tick regardless of metadata wedge")

	// Unblock metadata so Stop() can drain cleanly.
	close(metaBlock)
	observer.Stop()
	a.False(observer.isRunning, "observer should no longer be running after Stop")
}
