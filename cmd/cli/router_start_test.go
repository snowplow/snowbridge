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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/failure"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

func TestRouter_Start_GracefulShutdownWithPartialBatches(t *testing.T) {
	// Create mock targets with large batch sizes so messages don't auto-flush
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  100,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour - won't trigger during test
	}
	target, targetDriver := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, filterDriver := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	failureTarget, failureDriver := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	assert.False(t, targetDriver.IsOpened(), "Target should not be opened before Start()")
	assert.False(t, filterDriver.IsOpened(), "FilterTarget should not be opened before Start()")
	assert.False(t, failureDriver.IsOpened(), "FailureTarget should not be opened before Start()")

	// Create failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

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
		FailureTarget:        failureTarget,
		FailureParser:        failureParser,
		maxTargetSize:        1000000,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	var startWg sync.WaitGroup
	startWg.Go(router.Start)

	// Send partial batches that won't trigger auto-flush
	now := time.Now()
	invalidMsg := &models.Message{Data: []byte("invalid1"), PartitionKey: "key", TimePulled: now}
	invalidMsg.SetError(errors.New("test error"))

	// Send transformed messages
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result1"), PartitionKey: "success", TimePulled: now},
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result2"), PartitionKey: "invalid", TimePulled: now}, // Will be rejected by target as invalid
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result3"), PartitionKey: "invalid", TimePulled: now}, // Will be rejected by target as invalid
		nil,
		nil,
	)

	// Send filtered messages
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered1"), PartitionKey: "success", TimePulled: now},
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered2"), PartitionKey: "invalid", TimePulled: now}, // Will be rejected by filter target as invalid
		nil,
	)

	// Send invalid message
	transformationOutput <- models.NewTransformationResult(
		nil,
		nil,
		invalidMsg,
	)

	// Give router time to process messages
	time.Sleep(50 * time.Millisecond)

	assert.True(t, targetDriver.IsOpened())
	assert.True(t, filterDriver.IsOpened())
	assert.True(t, failureDriver.IsOpened())

	// Verify no batches sent yet (partial batches not flushed)
	assert.Equal(t, 0, len(targetDriver.GetReceivedBatches()), "Target should have 0 batches before shutdown")
	assert.Equal(t, 0, len(filterDriver.GetReceivedBatches()), "FilterTarget should have 0 batches before shutdown")
	assert.Equal(t, 0, len(failureDriver.GetReceivedBatches()), "FailureTarget should have 0 batches before shutdown")

	// Close transformationOutput to trigger graceful shutdown
	close(transformationOutput)

	// Wait for Start() to complete with timeout
	waitWithTimeout(t, &startWg, 5*time.Second, "Router Start() graceful shutdown")

	// Verify partial batches were flushed on all targets
	targetBatches := targetDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(targetBatches), "Target should have 1 batch after shutdown") {
		t.FailNow()
	}
	assert.Equal(t, 3, len(targetBatches[0]), "Target batch should have 3 messages")

	filterBatches := filterDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(filterBatches), "FilterTarget should have 1 batch after shutdown") {
		t.FailNow()
	}
	assert.Equal(t, 2, len(filterBatches[0]), "FilterTarget batch should have 2 messages")

	// FailureTarget receives: 1 from transformation invalid + 2 from target invalids + 1 from filter target invalid = 4 total
	failureBatches := failureDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(failureBatches), "FailureTarget should have 1 batch after shutdown") {
		t.FailNow()
	}
	assert.Equal(t, 4, len(failureBatches[0]), "FailureTarget batch should have 4 messages (1 transformation invalid + 2 target invalids + 1 filter target invalid)")

	// Verify targets were closed on shutdown
	assert.False(t, targetDriver.IsOpened(), "Target should be closed after shutdown")
	assert.False(t, filterDriver.IsOpened(), "FilterTarget should be closed after shutdown")
	assert.False(t, failureDriver.IsOpened(), "FailureTarget should be closed after shutdown")

	// Verify invalidChannel was closed
	_, ok := <-invalidChannel
	assert.False(t, ok, "invalidChannel should be closed after shutdown")
}

func TestRouter_Start_GracefulShutdownWithInFlightWrites(t *testing.T) {
	// Create mock targets that will have in-flight writes
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  2, // Small batch size to trigger auto-flush
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000,
	}
	target, targetDriver := createMockTargetWithConfig(1, batchingConfig) // Small throttle = 1 concurrent write
	defer target.Ticker.Stop()

	filterTarget, filterDriver := createMockTargetWithConfig(1, batchingConfig)
	defer filterTarget.Ticker.Stop()

	failureTarget, failureDriver := createMockTargetWithConfig(1, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Verify targets are not opened initially
	assert.False(t, targetDriver.IsOpened(), "Target should not be opened before Start()")
	assert.False(t, filterDriver.IsOpened(), "FilterTarget should not be opened before Start()")
	assert.False(t, failureDriver.IsOpened(), "FailureTarget should not be opened before Start()")

	// Create failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

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
		FailureTarget:        failureTarget,
		FailureParser:        failureParser,
		maxTargetSize:        1000000,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	var startWg sync.WaitGroup
	startWg.Go(router.Start)

	// Send enough messages to trigger auto-flush (will start slow writes)
	// Our mock delays the writes by 500ms if the partition key is set to "slow"
	now := time.Now()
	invalidMsg1 := &models.Message{Data: []byte("invalid1"), PartitionKey: "slow", TimePulled: now}
	invalidMsg1.SetError(errors.New("test error"))
	invalidMsg2 := &models.Message{Data: []byte("invalid2"), PartitionKey: "slow", TimePulled: now}
	invalidMsg2.SetError(errors.New("test error"))

	// Send transformed messages
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result1"), PartitionKey: "slow", TimePulled: now}, // Succeeds after delay
		nil,
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		&models.Message{Data: []byte("result2"), PartitionKey: "invalid", TimePulled: now}, // Rejected by target as invalid
		nil,
		nil,
	)

	// Send filtered messages
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered1"), PartitionKey: "slow", TimePulled: now}, // Succeeds after delay
		nil,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		&models.Message{Data: []byte("filtered2"), PartitionKey: "invalid", TimePulled: now}, // Rejected by filter target as invalid
		nil,
	)

	// Send invalid messages
	transformationOutput <- models.NewTransformationResult(
		nil,
		nil,
		invalidMsg1,
	)
	transformationOutput <- models.NewTransformationResult(
		nil,
		nil,
		invalidMsg2,
	)
	// Give router time to process messages
	time.Sleep(50 * time.Millisecond)

	assert.True(t, targetDriver.IsOpened())
	assert.True(t, filterDriver.IsOpened())
	assert.True(t, failureDriver.IsOpened())

	// Close transformationOutput to trigger shutdown (while writes are in-flight)
	close(transformationOutput)

	// Start() should wait for all in-flight writes to complete
	waitWithTimeout(t, &startWg, 5*time.Second, "Router Start() graceful shutdown with in-flight writes")

	// Verify all writes completed despite being in-flight during shutdown
	targetBatches := targetDriver.GetReceivedBatches()
	assert.Equal(t, 1, len(targetBatches), "Target should have completed in-flight write")
	assert.Equal(t, 2, len(targetBatches[0]), "Target batch should have 2 messages (1 slow success + 1 invalid)")

	filterBatches := filterDriver.GetReceivedBatches()
	assert.Equal(t, 1, len(filterBatches), "FilterTarget should have completed in-flight write")
	assert.Equal(t, 2, len(filterBatches[0]), "FilterTarget batch should have 2 messages (1 slow success + 1 invalid)")

	failureBatches := failureDriver.GetReceivedBatches()
	assert.Equal(t, 2, len(failureBatches), "FailureTarget should have completed 2 in-flight writes")
	assert.Equal(t, 2, len(failureBatches[0]), "First batch should have 2 transformation invalids")
	assert.Equal(t, 2, len(failureBatches[1]), "Second batch should have 2 target invalids (from target + filter target)")

	// Verify targets were closed on shutdown
	assert.False(t, targetDriver.IsOpened(), "Target should be closed after shutdown")
	assert.False(t, filterDriver.IsOpened(), "FilterTarget should be closed after shutdown")
	assert.False(t, failureDriver.IsOpened(), "FailureTarget should be closed after shutdown")

	// Verify invalidChannel was closed
	_, ok := <-invalidChannel
	assert.False(t, ok, "invalidChannel should be closed after shutdown")
}
