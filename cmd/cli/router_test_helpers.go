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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

// receivedBatch holds a batch of messages and the timestamp when it was received
type receivedBatch struct {
	messages  []*models.Message
	timestamp time.Time
}

// mockTargetDriver is a mock implementation of TargetDriver for testing
// PartitionKey values control test behavior:
//   - "slow": Delays write by 500ms (for testing in-flight writes)
//   - "fatal": Returns fatal error with all messages as failed
//   - "fail-for-N": Fails N times before succeeding (e.g., "fail-for-3")
//   - "invalid": Message becomes invalid
//   - "failed": Message becomes failed
//   - "write-fatal": Returns FatalWriteError on first call (tests setup-block guard)
//   - "write-fatal-transient": Returns plain error on call 1, FatalWriteError on call 2 (tests transient-block guard)
//   - default: Message succeeds
type mockTargetDriver struct {
	mu              sync.Mutex
	receivedBatches []receivedBatch
	batchingConfig  targetiface.BatchingConfig
	isOpened        bool
	callCounts      map[string]int
}

func (m *mockTargetDriver) GetDefaultConfiguration() any {
	return nil
}

func (m *mockTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return m.batchingConfig
}

func (m *mockTargetDriver) InitFromConfig(config any) error {
	return nil
}

func (m *mockTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, m.batchingConfig)
}

func (m *mockTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.receivedBatches = append(m.receivedBatches, receivedBatch{
		messages:  messages,
		timestamp: time.Now(),
	})

	// Check for fatal error first(tier-specific FatalWriteError triggers)
	for _, msg := range messages {
		switch msg.PartitionKey {
		case "fatal":
			// On fatal error, return all messages as failed
			return models.NewTargetWriteResult(nil, messages, nil), models.FatalWriteError{Err: errors.New("fatal error")}
		case "fatal-transient":
			// Call 1: plain error (not setup, not throttle — setup block exits, throttle check skipped, enters transient block)
			// Call 2: FatalWriteError (transient-block guard fires)
			m.callCounts[msg.PartitionKey]++
			if m.callCounts[msg.PartitionKey] == 1 {
				return models.NewTargetWriteResult(nil, messages, nil),
					fmt.Errorf("transient write error")
			}
			return models.NewTargetWriteResult(nil, messages, nil),
				models.FatalWriteError{Err: errors.New("fatal write error")}
		}
	}

	// Partition messages based on their PartitionKey
	var sent, failed, invalid []*models.Message

	for _, msg := range messages {
		switch {
		// Simulate slow writes for testing in-flight behavior
		case msg.PartitionKey == "slow":
			time.Sleep(500 * time.Millisecond)

			// Treat as successful after delay
			sent = append(sent, msg)
			if msg.AckFunc != nil {
				msg.AckFunc()
			}

		// This is a mechanism to allow us to configure n retries in our tests, and assert against their behaviours.
		// It checks for fail-for-n, decrements n, and when n reaches 0 it schedules for success.
		case strings.HasPrefix(msg.PartitionKey, "fail-for-"):
			// Parse N from "fail-for-N"
			nStr := strings.TrimPrefix(msg.PartitionKey, "fail-for-")
			n, err := strconv.Atoi(nStr)
			if err != nil || n < 1 {
				// Invalid format, treat as regular partition key
				failed = append(failed, msg)
				break
			}

			// Decrement the counter and update partition key
			n--
			if n > 0 {
				msg.PartitionKey = fmt.Sprintf("fail-for-%d", n)
			} else {
				msg.PartitionKey = "success"
			}
			// Always return as failed - the updated partition key determines next behavior
			failed = append(failed, msg)

		case msg.PartitionKey == "invalid":
			//Set error on invalid messages for failure parser
			msg.SetError(errors.New("target rejected message as invalid"))
			invalid = append(invalid, msg)

		case msg.PartitionKey == "failed":
			failed = append(failed, msg)

		default:
			sent = append(sent, msg)
			// Ack successful messages
			// This is currently here, but the test setup covers it in case we move it
			if msg.AckFunc != nil {
				msg.AckFunc()
			}
		}
	}

	// Build error message if there are failures
	// This matches real target behavior - return an error when there are failures
	// Invalids alone are NOT an error (they're just messages that didn't meet requirements)
	var err error
	if len(failed) > 0 {
		err = fmt.Errorf("mock target write had %d failed and %d invalid messages", len(failed), len(invalid))
	}

	return models.NewTargetWriteResult(sent, failed, invalid), err
}

func (m *mockTargetDriver) Open() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isOpened = true
	return nil
}

func (m *mockTargetDriver) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isOpened = false
}

func (m *mockTargetDriver) GetID() string {
	return "mock-target"
}

func (m *mockTargetDriver) GetReceivedBatches() [][]*models.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Extract just the messages for backward compatibility
	batches := make([][]*models.Message, len(m.receivedBatches))
	for i, rb := range m.receivedBatches {
		batches[i] = rb.messages
	}
	return batches
}

func (m *mockTargetDriver) GetReceivedBatchesWithTimestamps() []receivedBatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedBatches
}

func (m *mockTargetDriver) IsOpened() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOpened
}

// createMockTarget creates a Target with a mock driver for testing
func createMockTarget(bufferSize int) (*targetiface.Target, *mockTargetDriver) {
	return createMockTargetWithConfig(bufferSize, targetiface.BatchingConfig{
		MaxBatchMessages:  100,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000000,
		FlushPeriodMillis: 3600000, // 1 hour
	})
}

// createMockTargetWithConfig creates a Target with a mock driver and custom batching config for testing
func createMockTargetWithConfig(bufferSize int, batchingConfig targetiface.BatchingConfig) (*targetiface.Target, *mockTargetDriver) {
	driver := &mockTargetDriver{
		receivedBatches: make([]receivedBatch, 0),
		batchingConfig:  batchingConfig,
		callCounts:      make(map[string]int),
	}

	// Create a ticker that won't fire during tests
	ticker := time.NewTicker(1 * time.Hour)
	ticker.Stop()

	target := &targetiface.Target{
		TargetDriver: driver,
		CurrentBatch: targetiface.CurrentBatch{
			Messages:  []*models.Message{},
			DataBytes: 0,
		},
		Throttle:     make(chan struct{}, bufferSize),
		WaitGroup:    &sync.WaitGroup{},
		Ticker:       ticker,
		TickerPeriod: time.Duration(batchingConfig.FlushPeriodMillis) * time.Millisecond,
	}

	return target, driver
}

// addAckNackTracking adds tracking functions to messages for testing acknowledgement behavior
func addAckNackTracking(messages []*models.Message) (ackedMessages, nackedMessages map[string]bool, mu *sync.Mutex) {
	ackedMessages = make(map[string]bool)
	nackedMessages = make(map[string]bool)
	mu = &sync.Mutex{}

	for _, msg := range messages {
		// Capture id in closure properly
		id := string(msg.Data)
		msg.AckFunc = func(capturedID string) func() {
			return func() {
				mu.Lock()
				defer mu.Unlock()
				ackedMessages[capturedID] = true
			}
		}(id)
		msg.NackFunc = func(capturedID string) func() {
			return func() {
				mu.Lock()
				defer mu.Unlock()
				nackedMessages[capturedID] = true
			}
		}(id)
	}

	return
}

// drainInvalidChannel reads all messages from the invalidChannel with a timeout between messages
// Returns all messages received before the timeout expires with no new messages
func drainInvalidChannel(ch chan *invalidMessages, timeoutBetweenMessages time.Duration) []*invalidMessages {
	var results []*invalidMessages

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return results
			}
			// Got a message, loop continues and timeout resets
			results = append(results, msg)
		case <-time.After(timeoutBetweenMessages):
			// No message received within timeout, we're done
			return results
		}
	}
}

// createMockCancel creates a mock cancel function that tracks whether it was called
func createMockCancel() (cancel func(), wasCalled func() bool) {
	var called atomic.Bool

	cancel = func() {
		called.Store(true)
	}

	wasCalled = func() bool {
		return called.Load()
	}

	return cancel, wasCalled
}

// waitWithTimeout waits for a sync.WaitGroup with a timeout
// If the timeout is exceeded, the test fails with a fatal error
func waitWithTimeout(t interface {
	Fatalf(format string, args ...interface{})
}, wg *sync.WaitGroup, timeout time.Duration, description string) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(timeout):
		t.Fatalf("%s timed out after %v", description, timeout)
	}
}

// mockMetrics is a metrics implementation that tracks call counts using atomics
type mockMetrics struct {
	targetWriteCount        atomic.Int32
	targetWriteInvalidCount atomic.Int32
	filteredCount           atomic.Int32
}

func (m *mockMetrics) TargetWrite(r *models.TargetWriteResult) {
	m.targetWriteCount.Add(int32(len(r.Sent)))
}

func (m *mockMetrics) TargetWriteInvalid(r *models.TargetWriteResult) {
	m.targetWriteInvalidCount.Add(int32(len(r.Sent)))
}

func (m *mockMetrics) TargetWriteFiltered(r *models.TargetWriteResult) {
	m.filteredCount.Add(int32(len(r.Sent)))
}

func (m *mockMetrics) GetTargetWriteCount() int {
	return int(m.targetWriteCount.Load())
}

func (m *mockMetrics) GetTargetWriteInvalidCount() int {
	return int(m.targetWriteInvalidCount.Load())
}

func (m *mockMetrics) GetFilteredCount() int {
	return int(m.filteredCount.Load())
}

func createMockMetrics() *mockMetrics {
	return &mockMetrics{}
}
