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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/failure"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

func TestRouteInvalid_BasicBatching(t *testing.T) {
	// Create mock failure target with batch size of 3
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	failureTarget, failureDriver := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Create messages with errors set (required for invalid transformation)
	now := time.Now()
	msgs := []*models.Message{
		{
			Data:         []byte("invalid1"),
			PartitionKey: "key1",
			TimePulled:   now,
		},
		{
			Data:         []byte("invalid2"),
			PartitionKey: "key2",
			TimePulled:   now,
		},
		{
			Data:         []byte("invalid3"),
			PartitionKey: "key3",
			TimePulled:   now,
		},
	}
	msgs[0].SetError(errors.New("error 1"))
	msgs[1].SetError(errors.New("error 2"))
	msgs[2].SetError(errors.New("error 3"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs,
	}

	// Wait for write to complete
	time.Sleep(100 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify failure target received exactly 1 batch
	failureBatches := failureDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(failureBatches), "Should have 1 batch") {
		t.FailNow()
	}

	// Verify batch has 3 messages
	if !assert.Equal(t, 3, len(failureBatches[0]), "Batch should have 3 messages") {
		t.FailNow()
	}

	// Parse the transformed bad-row JSON to extract original data
	// EventForwarding error format has structure: data.failure.latestState
	for i, msg := range failureBatches[0] {
		var badRow map[string]interface{}
		err := json.Unmarshal(msg.Data, &badRow)
		if err != nil {
			t.Fatalf("Failed to unmarshal bad row JSON: %v", err)
		}

		// Navigate to data.failure.latestState
		data, ok := badRow["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("Bad row missing data field")
		}

		failure, ok := data["failure"].(map[string]interface{})
		if !ok {
			t.Fatalf("Bad row data missing failure field")
		}

		latestState, ok := failure["latestState"].(string)
		if !ok {
			t.Fatalf("Bad row failure missing latestState field")
		}

		// Verify the original data is preserved in latestState
		expectedData := []string{"invalid1", "invalid2", "invalid3"}
		assert.Equal(t, expectedData[i], latestState, "Original data should be preserved in transformed message")
	}
}

func TestRouteInvalid_OversizedMessages(t *testing.T) {
	// Create mock failure target
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	failureTarget, failureDriver := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Create oversized messages
	now := time.Now()
	oversizedMsgs := []*models.Message{
		{
			Data:       []byte("oversized1"),
			TimePulled: now,
		},
		{
			Data:       []byte("oversized2"),
			TimePulled: now,
		},
		{
			Data:       []byte("oversized3"),
			TimePulled: now,
		},
	}

	invalidChannel <- &invalidMessages{
		Oversized: oversizedMsgs,
	}

	// Wait for writes to complete
	time.Sleep(100 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify failure target received 1 batch (oversized messages are batched like regular invalids)
	failureBatches := failureDriver.GetReceivedBatches()
	if !assert.Equal(t, 1, len(failureBatches), "Should have 1 batch (oversized messages batched together)") {
		t.FailNow()
	}

	// Verify batch contains all 3 oversized messages
	if !assert.Equal(t, 3, len(failureBatches[0]), "Batch should have 3 messages") {
		t.FailNow()
	}

	// Collect all payloads from the batch
	var receivedPayloads []string
	for _, msg := range failureBatches[0] {
		// Parse the transformed bad-row JSON to extract original data
		// EventForwarding size_violation format has structure: data.payload
		var badRow map[string]interface{}
		err := json.Unmarshal(msg.Data, &badRow)
		if err != nil {
			t.Fatalf("Failed to unmarshal bad row JSON: %v", err)
		}

		// Navigate to data.payload
		data, ok := badRow["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("Bad row missing data field")
		}

		payload, ok := data["payload"].(string)
		if !ok {
			t.Fatalf("Bad row data missing payload field")
		}

		receivedPayloads = append(receivedPayloads, payload)
	}

	// Verify all expected data is present (order independent)
	expectedData := []string{"oversized1", "oversized2", "oversized3"}
	assert.ElementsMatch(t, expectedData, receivedPayloads, "All oversized messages should be received")
}

func TestRouteInvalid_BatchByteLimit(t *testing.T) {
	// Create mock failure target with small byte limit
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  100,
		MaxBatchBytes:     100, // 100 byte limit
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	failureTarget, failureDriver := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Create messages with errors (will be transformed to much larger bad-row JSON)
	now := time.Now()
	msgs := []*models.Message{
		{Data: []byte("12345678901234567890"), PartitionKey: "key1", TimePulled: now},                    // 20 bytes
		{Data: []byte("12345678901234567890"), PartitionKey: "key2", TimePulled: now},                    // 20 bytes
		{Data: []byte("12345678901234567890"), PartitionKey: "key3", TimePulled: now},                    // 20 bytes
		{Data: []byte("123456789012345678901234567890123456789"), PartitionKey: "key4", TimePulled: now}, // 39 bytes
	}
	msgs[0].SetError(errors.New("error 1"))
	msgs[1].SetError(errors.New("error 2"))
	msgs[2].SetError(errors.New("error 3"))
	msgs[3].SetError(errors.New("error 4"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs,
	}

	// Wait for writes to complete
	time.Sleep(100 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify failure target received multiple batches (byte limit should trigger batching)
	failureBatches := failureDriver.GetReceivedBatches()

	// After transformation, messages become much larger (bad-row JSON wrapper)
	// Should receive multiple batches due to 100 byte limit
	if !assert.Greater(t, len(failureBatches), 1, "Should have more than 1 batch due to byte limit") {
		t.FailNow()
	}

	// Collect all payloads and verify all messages were received
	var receivedPayloads []string
	for _, batch := range failureBatches {
		for _, msg := range batch {
			var badRow map[string]interface{}
			err := json.Unmarshal(msg.Data, &badRow)
			if err != nil {
				t.Fatalf("Failed to unmarshal bad row JSON: %v", err)
			}

			// Navigate to data.failure.latestState
			data, ok := badRow["data"].(map[string]interface{})
			if !ok {
				t.Fatalf("Bad row missing data field")
			}

			failure, ok := data["failure"].(map[string]interface{})
			if !ok {
				t.Fatalf("Bad row data missing failure field")
			}

			latestState, ok := failure["latestState"].(string)
			if !ok {
				t.Fatalf("Bad row failure missing latestState field")
			}

			receivedPayloads = append(receivedPayloads, latestState)
		}
	}

	// Verify all 4 messages were received (order independent)
	expectedData := []string{"12345678901234567890", "12345678901234567890", "12345678901234567890", "123456789012345678901234567890123456789"}
	assert.ElementsMatch(t, expectedData, receivedPayloads, "All messages should be received across multiple batches")
}

func TestRouteInvalid_TickerFlush(t *testing.T) {
	// Create mock failure target with short flush period
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 100, // 100ms flush period
	}
	failureTarget, failureDriver := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, _ := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Send incomplete batch (2 invalid messages, less than MaxBatchMessages of 3)
	now := time.Now()
	msgs := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "key1", TimePulled: now},
		{Data: []byte("message2"), PartitionKey: "key2", TimePulled: now},
	}
	msgs[0].SetError(errors.New("error 1"))
	msgs[1].SetError(errors.New("error 2"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs,
	}

	// The mock's ticker is disabled at first, which allows us to test that each write call resets the ticker.
	// First, we assert that this is the case, to confirm we're testing what we think we are.
	// Wait longer than the flush period, and check results.
	time.Sleep(150 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify no batches sent yet (ticker hasn't fired because it's stopped)
	assert.Equal(t, 0, len(failureDriver.GetReceivedBatches()), "Should have 0 batches (ticker is stopped)")

	// Now send a full batch (3 invalid messages) which should trigger immediate write and reset the ticker
	msgs2 := []*models.Message{
		{Data: []byte("message3"), PartitionKey: "key3", TimePulled: now},
		{Data: []byte("message4"), PartitionKey: "key4", TimePulled: now},
		{Data: []byte("message5"), PartitionKey: "key5", TimePulled: now},
	}
	msgs2[0].SetError(errors.New("error 3"))
	msgs2[1].SetError(errors.New("error 4"))
	msgs2[2].SetError(errors.New("error 5"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs2,
	}

	// Wait for the full batch write to complete
	time.Sleep(50 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Wait a bit longer than the flush period for ticker to fire and flush the partial batch
	time.Sleep(120 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Get batches with timestamps
	batchesWithTimestamps := failureDriver.GetReceivedBatchesWithTimestamps()

	// Should have received 2 batches
	if !assert.Equal(t, 2, len(batchesWithTimestamps), "Should have 2 batches") {
		t.FailNow()
	}

	// First batch should be a full batch (3 messages) containing messages 3, 4, 5
	if !assert.Equal(t, 3, len(batchesWithTimestamps[0].messages), "First batch should have 3 messages") {
		t.FailNow()
	}

	// Second batch should have the partial batch (2 messages) flushed by ticker
	if !assert.Equal(t, 2, len(batchesWithTimestamps[1].messages), "Second batch should have 2 messages") {
		t.FailNow()
	}

	// Verify timestamps are roughly 100ms apart
	timeDiff := batchesWithTimestamps[1].timestamp.Sub(batchesWithTimestamps[0].timestamp)
	assert.Greater(t, timeDiff.Milliseconds(), int64(90), "Time difference should be at least 90ms")
	assert.Less(t, timeDiff.Milliseconds(), int64(150), "Time difference should be less than 150ms")

	// Parse messages to verify content
	var firstBatchPayloads, secondBatchPayloads []string

	for _, msg := range batchesWithTimestamps[0].messages {
		var badRow map[string]interface{}
		err := json.Unmarshal(msg.Data, &badRow)
		if err != nil {
			t.Fatalf("Failed to unmarshal bad row JSON: %v", err)
		}

		data, _ := badRow["data"].(map[string]interface{})
		failure, _ := data["failure"].(map[string]interface{})
		latestState, _ := failure["latestState"].(string)
		firstBatchPayloads = append(firstBatchPayloads, latestState)
	}

	for _, msg := range batchesWithTimestamps[1].messages {
		var badRow map[string]interface{}
		err := json.Unmarshal(msg.Data, &badRow)
		if err != nil {
			t.Fatalf("Failed to unmarshal bad row JSON: %v", err)
		}

		data, _ := badRow["data"].(map[string]interface{})
		failure, _ := data["failure"].(map[string]interface{})
		latestState, _ := failure["latestState"].(string)
		secondBatchPayloads = append(secondBatchPayloads, latestState)
	}

	// Verify first batch has messages 1, 2, 3 (first incomplete batch + first from second send)
	assert.ElementsMatch(t, []string{"message1", "message2", "message3"}, firstBatchPayloads, "First batch should contain messages 1, 2, 3")

	// Verify second batch has messages 4, 5 (remaining from second send, flushed by ticker)
	assert.ElementsMatch(t, []string{"message4", "message5"}, secondBatchPayloads, "Second batch should contain messages 4, 5")
}

func TestRouteInvalid_FatalError(t *testing.T) {
	// Create mock failure target with small batch size to trigger write quickly
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	failureTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Create invalid messages where one will trigger a fatal error after transformation
	now := time.Now()
	msgs := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success", TimePulled: now},
		{Data: []byte("message2"), PartitionKey: "success", TimePulled: now},
		{Data: []byte("message3"), PartitionKey: "fatal", TimePulled: now}, // Will trigger fatal error
	}
	msgs[0].SetError(errors.New("error 1"))
	msgs[1].SetError(errors.New("error 2"))
	msgs[2].SetError(errors.New("error 3"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs,
	}

	// Wait for async write to complete
	time.Sleep(50 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify cancel was called due to fatal error
	assert.True(t, wasCancelCalled(), "Cancel should be called due to fatal error in failure target write")
}

func TestRouteInvalid_InvalidResult(t *testing.T) {
	// Create mock failure target with small batch size to trigger write quickly
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	}
	failureTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create event forwarding failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router
	invalidChannel := make(chan *invalidMessages, 10)
	mockCancel, wasCancelCalled := createMockCancel()

	router := &Router{
		invalidChannel: invalidChannel,
		cancel:         mockCancel,
		FailureTarget:  failureTarget,
		FailureParser:  failureParser,
		maxTargetSize:  1000000,
		metrics:        createMockMetrics(),
	}

	// Start RouteInvalid in goroutine
	go router.RouteInvalid()

	// Create invalid messages where one will be returned as "invalid" by the failure target
	// This simulates the failure target itself producing invalid results
	now := time.Now()
	msgs := []*models.Message{
		{Data: []byte("message1"), PartitionKey: "success", TimePulled: now},
		{Data: []byte("message2"), PartitionKey: "invalid", TimePulled: now}, // Will be returned as invalid
		{Data: []byte("message3"), PartitionKey: "success", TimePulled: now},
	}
	msgs[0].SetError(errors.New("error 1"))
	msgs[1].SetError(errors.New("error 2"))
	msgs[2].SetError(errors.New("error 3"))

	invalidChannel <- &invalidMessages{
		Invalid: msgs,
	}

	// Wait for async write to complete
	time.Sleep(50 * time.Millisecond)
	router.FailureTarget.WaitGroup.Wait()

	// Verify cancel was called because failure target produced invalid messages (fatal condition)
	assert.True(t, wasCancelCalled(), "Cancel should be called when failure target produces invalid messages")

	// Verify no messages went to invalid channel (failure target invalids are treated as fatal)
	invalids := drainInvalidChannel(invalidChannel, 10*time.Millisecond)
	assert.Equal(t, 0, len(invalids), "Invalid messages should not be sent to invalid channel for failure target")
}
