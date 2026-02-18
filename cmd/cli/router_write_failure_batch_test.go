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

func TestWriteFailureBatch_Basic(t *testing.T) {
	// Create mock failure target
	failureTarget, mockDriver := createMockTarget(10)

	// Create router
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		FailureTarget:  failureTarget,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig:    &config.RetryConfig{
			// empty: retry config is not configurable for failure target
		},
		metrics: createMockMetrics(),
	}

	// Create test messages with partition keys to control outcome
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "invalid"},
		{Data: []byte("message3"), PartitionKey: "fail-for-1"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteFailureBatch
	router.WriteFailureBatch(testMessages, func(*models.TargetWriteResult) {})

	// Wait for async write to complete
	failureTarget.WaitGroup.Wait()

	// Verify Write was called with correct data
	receivedBatches := mockDriver.GetReceivedBatches()

	if !assert.Equal(t, 2, len(receivedBatches), "Expected 2 batches to be received") {
		t.FailNow()
	}
	if !assert.Equal(t, 3, len(receivedBatches[0]), "Expected first batch to contain 3 messages") {
		t.FailNow()
	}

	// Verify message content
	for i, msg := range receivedBatches[0] {
		assert.Equal(t, testMessages[i].Data, msg.Data, "Message data should match")
	}

	// Verify cancel was called because failure target produced invalid messages (fatal condition)
	assert.True(t, wasCancelCalled(), "Cancel should be called when failure target produces invalid messages")

	// Verify no messages went to invalid channel (key difference from WriteBatch)
	invalids := drainInvalidChannel(router.invalidChannel, 10*time.Millisecond)
	assert.Equal(t, 0, len(invalids), "Invalid messages should not be sent to invalid channel for failure target")

	// Verify acking/nacking behavior
	mu.Lock()
	defer mu.Unlock()

	// Successful message should be acked, not nacked
	assert.True(t, ackedMessages["message1"], "Successful message should be acked")
	assert.False(t, nackedMessages["message1"], "Successful message should not be nacked")

	// Invalid messages are treated as fatal for failure target - should be nacked
	assert.False(t, ackedMessages["message2"], "Invalid message should not be acked")
	assert.True(t, nackedMessages["message2"], "Invalid message should be nacked (treated as fatal for failure target)")

	// Failed message should also be nacked when there's a fatal condition (invalid messages)
	assert.True(t, ackedMessages["message3"], "Failed message should be acked")
	assert.False(t, nackedMessages["message3"], "Failed message should not be nacked")
}

// This test specifies the desired retry behaviour for failure target. The "fail-for-n" partition keys tell our mocks how many times to fail a given event.
func TestWriteFailureBatch_Retry(t *testing.T) {
	// Create mock failure target
	failureTarget, mockDriver := createMockTarget(10)

	// Create router
	mockCancel, _ := createMockCancel()

	router := &Router{
		FailureTarget:  failureTarget,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig:    &config.RetryConfig{
			// empty: retry config is not configurable for failure target
		},
		metrics: createMockMetrics(),
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

	// Call WriteFailureBatch
	router.WriteFailureBatch(testMessages, func(*models.TargetWriteResult) {})

	// Wait for async write to complete
	failureTarget.WaitGroup.Wait()

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

	// Eventually successful messages should not be nacked
	assert.False(t, nackedMessages["message1"], "Successful messages should not be nacked")
	assert.False(t, nackedMessages["message2"], "Successful messages should not be nacked")
	assert.False(t, nackedMessages["message4"], "Successful messages should not be nacked")
	assert.False(t, nackedMessages["message5"], "Successful messages should not be nacked")
	assert.False(t, nackedMessages["message6"], "Successful messages should not be nacked")

	// Invalid message should be nacked - treated as fatal
	assert.True(t, nackedMessages["message3"], "Invalid message should be nacked")
}

func TestWriteFailureBatch_FatalError(t *testing.T) {
	// Create mock failure target
	failureTarget, _ := createMockTarget(10)

	// Create router
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		FailureTarget:  failureTarget,
		invalidChannel: make(chan *invalidMessages, 10),
		cancel:         mockCancel,
		retryConfig: &config.RetryConfig{
			Setup:     &config.SetupRetryConfig{Delay: 100, MaxAttempts: 1},
			Transient: &config.TransientRetryConfig{Delay: 100, MaxAttempts: 1},
			Throttle:  &config.ThrottleRetryConfig{Delay: 100, MaxAttempts: 1},
		},
	}

	// Create test messages including one that triggers a fatal error
	testMessages := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success"},
		{Data: []byte("message2"), PartitionKey: "failed"},
		{Data: []byte("message3"), PartitionKey: "fatal"},
	}

	// Add ack/nack tracking
	ackedMessages, nackedMessages, mu := addAckNackTracking(testMessages)

	// Call WriteFailureBatch
	router.WriteFailureBatch(testMessages, func(*models.TargetWriteResult) {})

	// Wait for async write to complete
	failureTarget.WaitGroup.Wait()

	// Verify cancel was called due to fatal error
	assert.True(t, wasCancelCalled(), "Cancel should be called due to fatal error in failure target write")

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
}
