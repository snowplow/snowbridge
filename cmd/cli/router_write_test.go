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
)

func TestWriteBatch_Basic(t *testing.T) {
	// Create mock target
	target, mockDriver := createMockTarget(10)

	// Create router
	mockCancel, _ := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
		metrics: createMockMetrics(),
	}

	// Create test messages with partition keys to control outcome
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "invalid"},
		{Data: []byte("message3"), PartitionKey: "failed"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteBatch
	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})

	// Wait for async write to complete
	target.WaitGroup.Wait()

	// Verify Write was called with correct data
	receivedBatches := mockDriver.GetReceivedBatches()
	// We get 2 batches because failed will always have 1 retry - we can't configure otherwise at present. Retry behaviour is tested below.
	if !assert.Equal(t, 2, len(receivedBatches), "Expected one batch to be received") {
		t.FailNow()
	}
	if !assert.Equal(t, 3, len(receivedBatches[0]), "Expected batch to contain 3 messages") {
		t.FailNow()
	}

	// Verify message content
	for i, msg := range receivedBatches[0] {
		assert.Equal(t, testMessages[i].Data, msg.Data, "Message data should match")
	}

	// Verify invalid messages were sent to invalid channel
	invalids := drainInvalidChannel(router.invalidChannel, 10*time.Millisecond)
	if !assert.Equal(t, 1, len(invalids), "Expected 1 invalid message batch") {
		t.FailNow()
	}
	assert.Equal(t, 1, len(invalids[0].Invalid), "Expected 1 invalid message")
	assert.Equal(t, testMessages[1].Data, invalids[0].Invalid[0].Data, "Invalid message should match")

	// Verify acking/nacking behavior
	mu.Lock()
	defer mu.Unlock()

	// Successful message should be acked, not nacked
	assert.True(t, ackedMessages["message1"], "Successful message should be acked")
	assert.False(t, nackedMessages["message1"], "Successful message should not be nacked")

	// Invalid message should not be acked or nacked
	assert.False(t, ackedMessages["message2"], "Invalid message should not be acked")
	assert.False(t, nackedMessages["message2"], "Invalid message should not be nacked")

	// Failed message should not be acked or nacked (nacking happens only on fatal errors)
	assert.False(t, ackedMessages["message3"], "Failed message should not be acked")
	assert.True(t, nackedMessages["message3"], "Failed message should be nacked after retries exhausted")
}

func TestWriteBatch_FatalError(t *testing.T) {
	// Create mock target
	target, mockDriver := createMockTarget(10)

	// Create router
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 2},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 2},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 2},
		},
		metrics: createMockMetrics(),
	}

	// Create test messages including one that triggers a fatal error
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "failed"},
		{Data: []byte("message3"), PartitionKey: "fatal"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteBatch
	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})

	// Wait for async write to complete
	target.WaitGroup.Wait()

	// Setup block fires FatalWriteError on call 1 — guard exits immediately, no further retry tiers
	assert.Equal(t, 1, len(mockDriver.GetReceivedBatches()), "Expected exactly 1 write call for setup-block FatalWriteError")

	// Verify cancel was called due to fatal error
	assert.True(t, wasCancelCalled(), "Cancel should be called due to fatal error in target write")

	// Verify acking/nacking behavior
	mu.Lock()
	defer mu.Unlock()

	// When there's a fatal error, all messages in the batch are nacked
	assert.False(t, ackedMessages["message1"], "Message should not be acked due to fatal error")
	assert.True(t, nackedMessages["message1"], "Message should be nacked due to fatal error")

	assert.False(t, ackedMessages["message2"], "Message should not be acked due to fatal error")
	assert.True(t, nackedMessages["message2"], "Message should be nacked due to fatal error")

	assert.False(t, ackedMessages["message3"], "Message should not be acked due to fatal error")
	assert.True(t, nackedMessages["message3"], "Message should be nacked due to fatal error")

	// sendToInvalid=false — invalid channel must receive nothing
	select {
	case v := <-router.invalidChannel:
		t.Fatalf("Expected empty invalid channel, got: %v", v)
	default:
	}
}

func TestWriteBatch_FatalWriteError_Transient(t *testing.T) {
	target, mockDriver := createMockTarget(10)
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 1),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 2},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 2},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 2},
		},
		metrics: createMockMetrics(),
	}

	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "failed"},
		{Data: []byte("message1"), PartitionKey: "fatal-transient"},
	}

	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})
	target.WaitGroup.Wait()

	// Call 1: plain error (setup block), Call 2: FatalWriteError (transient block) — 2 total
	assert.Equal(t, 2, len(mockDriver.GetReceivedBatches()), "Expected exactly 2 write calls for transient-block FatalWriteError")

	// Verify cancel was called due to fatal error
	assert.True(t, wasCancelCalled(), "Cancel should be called due to fatal error in target write")

	// sendToInvalid=false — invalid channel must receive nothing
	select {
	case v := <-router.invalidChannel:
		t.Fatalf("Expected empty invalid channel, got: %v", v)
	default:
	}
}

// This test specifies the desired retry behaviour. The "fail-for-n" partition keys tell our mocks how many times to fail a given event.
func TestWriteBatch_Retry(t *testing.T) {
	// Create mock target
	target, mockDriver := createMockTarget(10)

	// Create router
	mockCancel, _ := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 1, MaxAttempts: 5},
			Transient: &config.TransientRetryConfig{Delay: 1, MaxAttempts: 5},
			Throttle:  &config.ThrottleRetryConfig{Delay: 1, MaxAttempts: 5},
		},
	}

	// Create test messages - some will succeed, some will fail and need retry, some invalid
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "fail-for-1"},
		{Data: []byte("message3"), PartitionKey: "invalid"},
		{Data: []byte("message4"), PartitionKey: "fail-for-2"},
		{Data: []byte("message5"), PartitionKey: "fail-for-3"},
		{Data: []byte("message6"), PartitionKey: "success"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteBatch
	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})

	time.Sleep(50 * time.Millisecond)
	// Wait for async write to complete
	target.WaitGroup.Wait()

	// Verify Write was called 4 times (initial + 3 retries)
	receivedBatches := mockDriver.GetReceivedBatches()
	if !assert.Equal(t, 4, len(receivedBatches), "Expected four Write calls (initial + 3 retries)") {
		t.FailNow()
	}

	// Verify first batch contained all 6 messages
	if !assert.Equal(t, 6, len(receivedBatches[0]), "First batch should contain all 6 messages") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(receivedBatches[0][0].Data))
	assert.Equal(t, "message2", string(receivedBatches[0][1].Data))
	assert.Equal(t, "message3", string(receivedBatches[0][2].Data))
	assert.Equal(t, "message4", string(receivedBatches[0][3].Data))
	assert.Equal(t, "message5", string(receivedBatches[0][4].Data))
	assert.Equal(t, "message6", string(receivedBatches[0][5].Data))

	// Verify second batch (first retry) contains 3 failed messages
	if !assert.Equal(t, 3, len(receivedBatches[1]), "Second batch should contain 3 failed messages") {
		t.FailNow()
	}
	assert.Equal(t, "message2", string(receivedBatches[1][0].Data))
	assert.Equal(t, "message4", string(receivedBatches[1][1].Data))
	assert.Equal(t, "message5", string(receivedBatches[1][2].Data))

	// Verify third batch (second retry) contains 2 failed messages
	if !assert.Equal(t, 2, len(receivedBatches[2]), "Third batch should contain 2 failed messages") {
		t.FailNow()
	}
	assert.Equal(t, "message4", string(receivedBatches[2][0].Data))
	assert.Equal(t, "message5", string(receivedBatches[2][1].Data))

	// Verify fourth batch (third retry) contains 1 failed message
	if !assert.Equal(t, 1, len(receivedBatches[3]), "Fourth batch should contain 1 failed message") {
		t.FailNow()
	}
	assert.Equal(t, "message5", string(receivedBatches[3][0].Data))

	// Verify acking behavior - all messages should eventually be acked
	// (assuming the retry succeeds, which our mock does by default)
	mu.Lock()
	defer mu.Unlock()

	assert.True(t, ackedMessages["message1"], "message1 should be acked")
	assert.True(t, ackedMessages["message2"], "message2 should eventually be acked after retry")
	assert.True(t, ackedMessages["message4"], "message4 should eventually be acked after retries")
	assert.True(t, ackedMessages["message5"], "message5 should eventually be acked after retries")
	assert.True(t, ackedMessages["message6"], "message6 should be acked")

	// No messages should be nacked (all retries succeeded)
	assert.False(t, nackedMessages["message1"], "No messages should be nacked")
	assert.False(t, nackedMessages["message2"], "No messages should be nacked")
	assert.False(t, nackedMessages["message3"], "No messages should be nacked")
	assert.False(t, nackedMessages["message4"], "No messages should be nacked")
	assert.False(t, nackedMessages["message5"], "No messages should be nacked")
	assert.False(t, nackedMessages["message6"], "No messages should be nacked")
}

func TestWriteBatch_RetryExhausted(t *testing.T) {
	// Create mock target
	target, mockDriver := createMockTarget(10)

	// Create router with 1 retry attempt
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 1, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 1, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 1, MaxAttempts: 1},
		},
	}

	// Create test message that always fails
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "failed"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteBatch
	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})

	time.Sleep(50 * time.Millisecond)
	// Wait for async write to complete
	target.WaitGroup.Wait()

	// Verify Write was called twice (initial + 1 retry)
	receivedBatches := mockDriver.GetReceivedBatches()
	if !assert.Equal(t, 2, len(receivedBatches), "Expected two Write calls (initial + 1 retry)") {
		t.FailNow()
	}

	// Verify both batches contained the same message
	if !assert.Equal(t, 1, len(receivedBatches[0]), "First batch should contain 1 message") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(receivedBatches[0][0].Data))

	if !assert.Equal(t, 1, len(receivedBatches[1]), "Second batch should contain 1 message") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(receivedBatches[1][0].Data))

	assert.True(t, wasCancelCalled(), "Cancel should be called after exhausting all retries")

	// Verify acking/nacking behavior
	mu.Lock()
	defer mu.Unlock()

	// Message should not be acked (all retries exhausted)
	assert.False(t, ackedMessages["message1"], "Message should not be acked when retries exhausted")
	// Message should be nacked (all retries exhausted)
	assert.True(t, nackedMessages["message1"], "Message should be nacked when retries exhausted")
}

func TestWriteBatch_RetryExhausted_InvalidAfterMax(t *testing.T) {
	// Create mock target
	target, mockDriver := createMockTarget(10)

	// Create router with 1 retry attempt and InvalidAfterMax enabled
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		Target:         target,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 1, MaxAttempts: 1, InvalidAfterMax: true},
			Transient: &config.TransientRetryConfig{Delay: 1, MaxAttempts: 1, InvalidAfterMax: true},
			Throttle:  &config.ThrottleRetryConfig{Delay: 1, MaxAttempts: 1, InvalidAfterMax: true},
		},
	}

	// Create test message that always fails
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "failed"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteBatch
	router.WriteBatch(testMessages, target, func(*models.TargetWriteResult) {})

	time.Sleep(50 * time.Millisecond)
	// Wait for async write to complete
	target.WaitGroup.Wait()

	// Verify Write was called twice (initial + 1 retry)
	receivedBatches := mockDriver.GetReceivedBatches()
	if !assert.Equal(t, 2, len(receivedBatches), "Expected two Write calls (initial + 1 retry)") {
		t.FailNow()
	}

	// Verify both batches contained the same message
	if !assert.Equal(t, 1, len(receivedBatches[0]), "First batch should contain 1 message") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(receivedBatches[0][0].Data))

	if !assert.Equal(t, 1, len(receivedBatches[1]), "Second batch should contain 1 message") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(receivedBatches[1][0].Data))

	// Verify cancel was NOT called when InvalidAfterMax is true (messages go to invalid channel instead)
	assert.False(t, wasCancelCalled(), "Cancel should not be called when InvalidAfterMax is enabled")

	// Verify invalid message was sent to invalid channel
	invalids := drainInvalidChannel(router.invalidChannel, 10*time.Millisecond)
	if !assert.Equal(t, 1, len(invalids), "Expected 1 invalid message batch") {
		t.FailNow()
	}
	if !assert.Equal(t, 1, len(invalids[0].Invalid), "Expected 1 invalid message") {
		t.FailNow()
	}
	assert.Equal(t, "message1", string(invalids[0].Invalid[0].Data))

	// Verify acking/nacking behavior
	mu.Lock()
	defer mu.Unlock()

	// When InvalidAfterMax is true, failed messages are sent to invalid but NOT nacked
	assert.False(t, ackedMessages["message1"], "Message should not be acked when sent to invalid")
	assert.False(t, nackedMessages["message1"], "Message should not be nacked when InvalidAfterMax is true")
}
