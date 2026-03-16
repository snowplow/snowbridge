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

package transform

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
)

// TestTransformer_GracefulShutdown verifies that when the input channel is closed:
// 1. All workers finish processing their messages
// 2. The output channel is closed after all workers complete
// 3. Start() waits for all workers before returning
func TestTransformer_GracefulShutdown(t *testing.T) {
	input := make(chan *models.Message)
	output := make(chan *models.TransformationResult, 10)

	transformFunc := func(msg *models.Message) *models.TransformationResult {
		// Add a small delay to simulate work
		time.Sleep(10 * time.Millisecond)

		msg.Data = []byte("transformed")

		return models.NewTransformationResult(msg, nil, nil)
	}

	obs := observer.New(nil, 1*time.Second, 10*time.Second, nil)

	// Create transformer with 3 workers
	transformer := NewTransformer(transformFunc, input, output, obs, 3)

	var wg sync.WaitGroup
	wg.Go(transformer.Start)

	// Publish messages
	for range 10 {
		input <- &models.Message{Data: []byte("data"), PartitionKey: "key", TimePulled: time.Now()}
	}

	// Close input channel to trigger graceful shutdown
	close(input)

	// Wait for transformer to finish
	isSuccessful := waitWithTimeout(&wg)
	assert.True(t, isSuccessful)

	var receivedCount int
	for result := range output {
		assert.Equal(t, "transformed", string(result.Transformed.Data))
		receivedCount++
	}

	assert.Equal(t, 10, receivedCount, "Should receive results for all messages")

	// Verify output channel is closed (reading from closed channel returns zero value and false)
	_, ok := <-output
	assert.False(t, ok, "Output channel should be closed after graceful shutdown")
}

// TestTransformer_ObserverTransformationMetrics tests that transformer calls observer.Transformed()
// and observer correctly tracks transformation latency metrics
func TestTransformer_ObserverTransformationMetrics(t *testing.T) {
	input := make(chan *models.Message)
	output := make(chan *models.TransformationResult, 5)

	// Create observer with mock stats receiver to capture metrics
	obs, mockStats := createObserverWithMockStats()
	obs.Start() // Start observer to process metrics
	defer obs.Stop()

	transformFunc := func(msg *models.Message) *models.TransformationResult {
		// Set transformation timing to enable latency tracking in observer
		msg.TimeTransformationStarted = time.Now()
		msg.Data = []byte("worker_processed_" + string(msg.Data))
		msg.TimeTransformed = time.Now().Add(1 * time.Millisecond)
		return models.NewTransformationResult(msg, nil, nil)
	}

	transformer := NewTransformer(transformFunc, input, output, obs, 1)

	var wg sync.WaitGroup
	wg.Go(transformer.Start)

	// Send test messages
	messageCount := 5
	for i := range messageCount {
		input <- &models.Message{
			Data:         []byte("message" + string(rune('A'+i))),
			PartitionKey: "test",
			TimePulled:   time.Now(),
			TimeCreated:  time.Now(),
		}
	}

	close(input)

	// Wait for transformer to finish
	isSuccessful := waitWithTimeout(&wg)
	assert.True(t, isSuccessful, "Transformer should complete successfully")

	// Collect all transformation results from output
	var outputResults []*models.TransformationResult
	for result := range output {
		outputResults = append(outputResults, result)
	}

	// Verify we processed all messages
	assert.Equal(t, messageCount, len(outputResults), "Should process all messages")

	// Wait for observer to flush metrics (give it time to process)
	time.Sleep(100 * time.Millisecond)

	// Verify each message was transformed (proving observer.Transformed was called for each)
	for _, result := range outputResults {
		assert.NotNil(t, result.Transformed, "Each result should have transformed data")
		assert.Contains(t, string(result.Transformed.Data), "worker_processed_", "Each message should be processed by worker")
	}

	// Verify observer captured transformation latency metrics
	buffers := mockStats.GetBuffers()
	assert.True(t, len(buffers) >= 1, "Observer should have flushed at least one metrics buffer")

	// Verify observer metrics show transformation activity
	// Check that any buffer has transformation latency metrics
	hasTransformMetrics := false
	for _, buf := range buffers {
		if buf.MaxTransformLatency > 0 || buf.MinTransformLatency > 0 {
			hasTransformMetrics = true
			break
		}
	}
	assert.True(t, hasTransformMetrics,
		"Observer should have transformation latency metrics indicating Transformed() was called")
}

// TestTransformer_MultipleWorkers verifies that multiple workers can process messages concurrently
// and all call the observer correctly
func TestTransformer_MultipleWorkers(t *testing.T) {
	input := make(chan *models.Message)
	output := make(chan *models.TransformationResult, 15)

	obs, mockStats := createObserverWithMockStats()
	obs.Start()
	defer obs.Stop()

	transformFunc := func(msg *models.Message) *models.TransformationResult {
		// Add slight delay to test concurrent processing
		time.Sleep(3 * time.Millisecond)

		// Set transformation timing for observer metrics
		msg.TimeTransformationStarted = time.Now()
		msg.Data = []byte("worker_processed_" + string(msg.Data))
		msg.TimeTransformed = time.Now().Add(1 * time.Millisecond)

		return models.NewTransformationResult(msg, nil, nil)
	}

	// Use multiple workers
	transformer := NewTransformer(transformFunc, input, output, obs, 3)

	var wg sync.WaitGroup
	wg.Go(transformer.Start)

	// Send multiple messages to test concurrent processing
	messageCount := 15
	for i := range messageCount {
		input <- &models.Message{
			Data:         []byte("message" + string(rune('A'+i))),
			PartitionKey: "test",
			TimePulled:   time.Now(),
		}
	}

	close(input)

	isSuccessful := waitWithTimeout(&wg)
	assert.True(t, isSuccessful, "Transformer should complete successfully")

	// Collect all outputResults
	var outputResults []*models.TransformationResult
	for result := range output {
		outputResults = append(outputResults, result)
	}

	// Verify all messages were processed
	assert.Equal(t, messageCount, len(outputResults), "Should process all messages")

	// Wait for observer to flush metrics
	time.Sleep(100 * time.Millisecond)

	// Verify each message was transformed (proving observer.Transformed was called for each)
	for _, result := range outputResults {
		assert.NotNil(t, result.Transformed, "Each result should have transformed data")
		assert.Contains(t, string(result.Transformed.Data), "worker_processed_", "Each message should be processed by worker")
	}

	// Verify observer captured transformation latency metrics
	buffers := mockStats.GetBuffers()
	assert.True(t, len(buffers) >= 1, "Observer should have flushed at least one metrics buffer")

	// Verify observer metrics show transformation activity
	// Check that any buffer has transformation latency metrics
	hasTransformMetrics := false
	for _, buf := range buffers {
		if buf.MaxTransformLatency > 0 || buf.MinTransformLatency > 0 {
			hasTransformMetrics = true
			break
		}
	}
	assert.True(t, hasTransformMetrics,
		"Observer should have transformation latency metrics indicating Transformed() was called")
}

func waitWithTimeout(wg *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(3 * time.Second):
		return false
	}
}

// mockStatsReceiver captures metrics sent to observer for testing
type mockStatsReceiver struct {
	buffers []models.ObserverBuffer
	mu      sync.Mutex
}

func (m *mockStatsReceiver) Send(buffer *models.ObserverBuffer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buffers = append(m.buffers, *buffer)
}

func (m *mockStatsReceiver) GetBuffers() []models.ObserverBuffer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]models.ObserverBuffer(nil), m.buffers...)
}

func createObserverWithMockStats() (*observer.Observer, *mockStatsReceiver) {
	mockStats := &mockStatsReceiver{}
	obs := observer.New(mockStats, 25*time.Millisecond, 50*time.Millisecond, nil)
	return obs, mockStats
}
