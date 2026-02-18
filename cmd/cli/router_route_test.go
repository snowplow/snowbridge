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

package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

func TestRoute_MixedMessages(t *testing.T) {
	// Create mock targets with small batch sizes to force batching behavior
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:     3,
		MaxBatchBytes:        1000000,
		MaxMessageBytes:      20,      // Small enough to test oversized, large enough for normal messages
		FlushPeriodMillis:    3600000, // 1 hour
		MaxConcurrentBatches: 1,       // Concurrency to 1 since our assertions care about order of writes
	}
	target, targetDriver := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, filterDriver := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	// Create router with transformation channel and invalid channel
	transformationOutput := make(chan *models.TransformationResult, 10)
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		cancel:               mockCancel,
		Target:               target,
		FilterTarget:         filterTarget,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	// Start Route in goroutine
	go router.Route()

	// Send first batch: 3 Result messages + 1 oversized (should trigger one full batch of 3, oversized goes to invalid)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result1"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result2"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result3"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("this_is_oversized_for_result"), PartitionKey: "success"}, // > 20 bytes
		nil,
		nil,
	)

	// Send second batch: 3 Filtered messages + 1 oversized (should trigger one full batch of 3, oversized goes to invalid)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered1"), PartitionKey: "success"},
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered2"), PartitionKey: "success"},
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered3"), PartitionKey: "success"},
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("this_is_oversized_for_filter"), PartitionKey: "success"}, // > 20 bytes
		nil,
	)

	// Send third batch: Invalid messages
	transformationOutput <- models.NewTransformationResult(
		nil,
		nil,
		&models.Message{Data: []byte("invalid1"), PartitionKey: "invalid"},
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		nil,
		&models.Message{Data: []byte("invalid2"), PartitionKey: "invalid"},
	)

	// Wait for writes to complete
	time.Sleep(50 * time.Millisecond)
	router.Target.WaitGroup.Wait()
	router.FilterTarget.WaitGroup.Wait()

	// Drain invalid messages from channel
	receivedInvalids := drainInvalidChannel(invalidChannel, 10*time.Millisecond)

	// Verify Target received Result messages in correct batches
	targetBatches := targetDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(targetBatches), "Target should receive 1 batch") {
		t.FailNow()
	}

	// First batch should have 3 messages (oversized excluded)
	if !assert.Equal(t, 3, len(targetBatches[0]), "First target batch should have 3 messages") {
		t.FailNow()
	}
	assert.Equal(t, "result1", string(targetBatches[0][0].Data))
	assert.Equal(t, "result2", string(targetBatches[0][1].Data))
	assert.Equal(t, "result3", string(targetBatches[0][2].Data))

	// Verify FilterTarget received Filtered messages in correct batches
	filterBatches := filterDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(filterBatches), "FilterTarget should receive 1 batch") {
		t.FailNow()
	}

	// First batch should have 3 messages (oversized excluded)
	if !assert.Equal(t, 3, len(filterBatches[0]), "First filter batch should have 3 messages") {
		t.FailNow()
	}
	assert.Equal(t, "filtered1", string(filterBatches[0][0].Data))
	assert.Equal(t, "filtered2", string(filterBatches[0][1].Data))
	assert.Equal(t, "filtered3", string(filterBatches[0][2].Data))

	// Verify invalid messages: should have 4 batches total (2 oversized, 2 invalid)
	assert.Equal(t, 4, len(receivedInvalids), "Should receive 4 invalid message batches")

	// Collect all oversized and invalid messages from the batches
	var allOversized []*models.Message
	var allInvalid []*models.Message

	for _, inv := range receivedInvalids {
		allOversized = append(allOversized, inv.Oversized...)
		allInvalid = append(allInvalid, inv.Invalid...)
	}

	// Verify we have exactly the expected oversized messages
	assert.Equal(t, 2, len(allOversized), "Should have 2 oversized messages total")

	// Check for specific oversized messages
	oversizedResultFound := false
	oversizedFilterFound := false
	for _, msg := range allOversized {
		switch string(msg.Data) {
		case "this_is_oversized_for_result":
			assert.False(t, oversizedResultFound, "Oversized result message should appear only once")
			oversizedResultFound = true
		case "this_is_oversized_for_filter":
			assert.False(t, oversizedFilterFound, "Oversized filter message should appear only once")
			oversizedFilterFound = true
		default:
			t.Errorf("Unexpected oversized message: %s", string(msg.Data))
		}
	}
	assert.True(t, oversizedResultFound, "Should have received oversized result message")
	assert.True(t, oversizedFilterFound, "Should have received oversized filter message")

	// Verify we have exactly the expected invalid messages
	assert.Equal(t, 2, len(allInvalid), "Should have 2 invalid messages total")

	// Check for specific invalid messages
	invalid1Found := false
	invalid2Found := false
	for _, msg := range allInvalid {
		switch string(msg.Data) {
		case "invalid1":
			assert.False(t, invalid1Found, "Invalid1 message should appear only once")
			invalid1Found = true
		case "invalid2":
			assert.False(t, invalid2Found, "Invalid2 message should appear only once")
			invalid2Found = true
		default:
			t.Errorf("Unexpected invalid message: %s", string(msg.Data))
		}
	}
	assert.True(t, invalid1Found, "Should have received invalid1 message")
	assert.True(t, invalid2Found, "Should have received invalid2 message")
}

func TestRoute_BatchByteLimit(t *testing.T) {
	// Create mock targets with 100 byte limit
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  100,
		MaxBatchBytes:     100,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	target, targetDriver := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	// Create router
	transformationOutput := make(chan *models.TransformationResult, 10)
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		cancel:               mockCancel,
		Target:               target,
		FilterTarget:         filterTarget,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	// Start Route in goroutine
	go router.Route()

	// Send 3x20 bytes (60 total), then 41 bytes
	// When 41 byte message arrives, 60+41=101 > 100, triggers flush of first 3
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("12345678901234567890"), PartitionKey: "success"}, // 20 bytes
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("12345678901234567890"), PartitionKey: "success"}, // 20 bytes
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("12345678901234567890"), PartitionKey: "success"}, // 20 bytes
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("12345678901234567890123456789012345678912"), PartitionKey: "success"}, // 41 bytes - should trigger write
		nil,
		nil,
	)

	// Sleep for a but in case waitgroup.Add() hasn't yet been called in the async write
	time.Sleep(50 * time.Millisecond)
	router.Target.WaitGroup.Wait()

	// Verify one batch was sent with first 3 messages (triggered by 4th exceeding limit)
	targetBatches := targetDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(targetBatches), "Should have 1 batch") {
		t.FailNow()
	}

	assert.Equal(t, 3, len(targetBatches[0]), "First batch should have 3 messages (60 bytes, before 4th triggered flush)")
}

func TestRoute_FatalError(t *testing.T) {
	// Create mock targets with small batch size to trigger write quickly
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	target, _ := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	// Create router
	transformationOutput := make(chan *models.TransformationResult, 10)
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		cancel:               mockCancel,
		Target:               target,
		FilterTarget:         filterTarget,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	// Start Route in goroutine
	go router.Route()

	// Send 3 messages where one will trigger a fatal error
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message1"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message2"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message3"), PartitionKey: "fatal"}, // Will trigger fatal error
		nil,
		nil,
	)

	// Wait for async write to complete
	time.Sleep(50 * time.Millisecond)
	router.Target.WaitGroup.Wait()

	// Verify cancel was called due to fatal error
	assert.True(t, wasCancelCalled(), "Cancel should be called due to fatal error in target write")
}

func TestRoute_TickerFlush(t *testing.T) {
	// Create mock targets with small batch size and short flush period
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 100, // 100ms flush period
	}
	target, targetDriver := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	// Create router
	transformationOutput := make(chan *models.TransformationResult, 10)
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		cancel:               mockCancel,
		Target:               target,
		FilterTarget:         filterTarget,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	// Start Route in goroutine
	go router.Route()

	// Send incomplete batch (2 messages, less than MaxBatchMessages of 3)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message1"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message2"), PartitionKey: "success"},
		nil,
		nil,
	)

	// The mock's ticker is disabled at first, which allows us to test that each write call resets the ticker.
	// First, we assert that this is the case, to confirm we're testing what we think we are.
	// Wait longer than the flush period, and check results.

	time.Sleep(150 * time.Millisecond)
	router.Target.WaitGroup.Wait()

	// Verify no batches sent yet (ticker hasn't fired because it's stopped)
	assert.Equal(t, 0, len(targetDriver.GetReceivedBatches()), "Should have 0 batches (ticker is stopped)")

	// Now send a full batch (3 messages) which should trigger immediate write and reset the ticker
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message3"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message4"), PartitionKey: "success"},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("message5"), PartitionKey: "success"},
		nil,
		nil,
	)

	// Wait for the full batch write to complete
	time.Sleep(50 * time.Millisecond)
	router.Target.WaitGroup.Wait()

	// Wait a bit longer than the flush period for ticker to fire and flush the partial batch
	time.Sleep(120 * time.Millisecond)
	router.Target.WaitGroup.Wait()

	// Get batches with timestamps
	batchesWithTimestamps := targetDriver.GetReceivedBatchesWithTimestamps()

	// Should have received 2 batches
	if !assert.Equal(t, 2, len(batchesWithTimestamps), "Should have 2 batches") {
		t.FailNow()
	}

	// First batch should be a full batch (3 messages) containing the first messages sent
	if !assert.Equal(t, 3, len(batchesWithTimestamps[0].messages), "First batch should have 3 messages") {
		t.FailNow()
	}

	// Verify first two messages sent (message1, message2) are in the first batch
	assert.Equal(t, "message1", string(batchesWithTimestamps[0].messages[0].Data))
	assert.Equal(t, "message2", string(batchesWithTimestamps[0].messages[1].Data))

	// Second batch should have remaining messages (2 messages, flushed by ticker)
	if !assert.Equal(t, 2, len(batchesWithTimestamps[1].messages), "Second batch should have 2 messages") {
		t.FailNow()
	}

	// Verify timestamps are roughly 100ms apart
	timeDiff := batchesWithTimestamps[1].timestamp.Sub(batchesWithTimestamps[0].timestamp)
	assert.Greater(t, timeDiff.Milliseconds(), int64(90), "Time difference should be at least 90ms")
	assert.Less(t, timeDiff.Milliseconds(), int64(150), "Time difference should be less than 150ms")

	batchesWithTimestamps = targetDriver.GetReceivedBatchesWithTimestamps()
	assert.Equal(t, 2, len(batchesWithTimestamps), "Should have still only 2 batches")
}
