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

func TestRouter_MetricsTracking(t *testing.T) {
	// Create mock targets
	batchingConfig := targetiface.BatchingConfig{
		MaxBatchMessages:  3,
		MaxBatchBytes:     1000000,
		MaxMessageBytes:   1000,    // Large enough for transformed invalid messages
		FlushPeriodMillis: 3600000, // 1 hour
	}
	target, _ := createMockTargetWithConfig(10, batchingConfig)
	defer target.Ticker.Stop()

	filterTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer filterTarget.Ticker.Stop()

	failureTarget, _ := createMockTargetWithConfig(10, batchingConfig)
	defer failureTarget.Ticker.Stop()

	// Create failure parser
	failureParser, err := failure.NewEventForwardingFailure(1000, "test", "0.1.0")
	if err != nil {
		t.Fatalf("Failed to create failure parser: %v", err)
	}

	// Create router with tracking metrics
	transformationOutput := make(chan *models.TransformationResult, 10)
	invalidChannel := make(chan *invalidMessages, 10)
	mockMetrics := createMockMetrics()
	mockCancel, _ := createMockCancel()

	router := &Router{
		transformationOutput: transformationOutput,
		invalidChannel:       invalidChannel,
		cancel:               mockCancel,
		AlertChannel:         make(chan error, 10),
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
		metrics: mockMetrics,
	}

	// Start router (starts both Route() and RouteInvalid() internally)
	var startWg sync.WaitGroup
	startWg.Go(router.Start)

	// Create messages
	now := time.Now()
	// Use timestamps in the past so latency values (TimeRequestFinished - TimePulled, etc.) are guaranteed > 0
	past := now.Add(-50 * time.Millisecond)
	collectorTstamp := now.Add(-100 * time.Millisecond)

	// Good messages - should trigger TargetWrite metrics
	// TimePulled/TimeCreated are set to past so proc/msg latencies are non-zero once TimeRequestFinished is stamped.
	// TimeTransformationStarted/TimeTransformed give non-zero transform latency.
	// CollectorTstamp gives non-zero E2E latency.
	goodMessages := []*models.Message{
		{Data: []byte("good1"), PartitionKey: "success", TimePulled: past, TimeCreated: past,
			TimeTransformationStarted: past, TimeTransformed: now, CollectorTstamp: collectorTstamp},
		{Data: []byte("good2"), PartitionKey: "success", TimePulled: past, TimeCreated: past,
			TimeTransformationStarted: past.Add(-1 * time.Minute), TimeTransformed: now, CollectorTstamp: collectorTstamp},
		{Data: []byte("good3"), PartitionKey: "success", TimePulled: past, TimeCreated: past,
			TimeTransformationStarted: past, TimeTransformed: now, CollectorTstamp: collectorTstamp},
	}

	// Filtered messages - should trigger Filtered metrics
	filteredMessages := []*models.Message{
		{Data: []byte("filtered1"), PartitionKey: "success", TimePulled: now, TimeCreated: now},
		{Data: []byte("filtered2"), PartitionKey: "success", TimePulled: now, TimeCreated: now},
	}

	// Invalid messages - should trigger TargetWriteInvalid metrics
	invalidMessages := []*models.Message{
		{Data: []byte("invalid1"), PartitionKey: "success", TimePulled: now, TimeCreated: now, OriginalData: []byte("invalid1")},
	}
	invalidMessages[0].SetError(errors.New("test error"))

	// Oversized messages - should trigger TargetWriteOversized metrics
	// Make a message that's clearly over 1000 bytes
	oversizedData := make([]byte, 2000)
	for i := range oversizedData {
		oversizedData[i] = 'x'
	}
	oversizedMessages := []*models.Message{
		{Data: oversizedData, PartitionKey: "success", TimePulled: now, TimeCreated: now, OriginalData: oversizedData},
	}

	// Send good messages
	for _, msg := range goodMessages {
		transformationOutput <- models.NewTransformationResult(msg, nil, nil)
	}

	// Send oversized message (treated as good/transformed)
	for _, msg := range oversizedMessages {
		transformationOutput <- models.NewTransformationResult(msg, nil, nil)
	}

	// Send filtered messages
	for _, msg := range filteredMessages {
		transformationOutput <- models.NewTransformationResult(nil, msg, nil)
	}

	// Send invalid messages
	for _, msg := range invalidMessages {
		transformationOutput <- models.NewTransformationResult(nil, nil, msg)
	}

	// Close transformationOutput to trigger shutdown
	close(transformationOutput)

	// Give time for message processing
	time.Sleep(100 * time.Millisecond)

	// Wait for router to complete
	waitWithTimeout(t, &startWg, 5*time.Second, "Router Start()")

	// Verify all metrics types were called
	assert.Equal(t, 3, mockMetrics.GetTargetWriteCount())
	assert.Equal(t, 2, mockMetrics.GetFilteredCount())
	assert.Equal(t, 2, mockMetrics.GetTargetWriteInvalidCount())

	buf := mockMetrics.GetWriteBuffer()
	// Processing latency
	assert.Greater(t, buf.MaxProcLatency, time.Duration(0))
	assert.Greater(t, buf.MinProcLatency, time.Duration(0))

	// Message latency
	assert.Greater(t, buf.MaxMsgLatency, time.Duration(0))
	assert.Greater(t, buf.MinMsgLatency, time.Duration(0))

	// E2E latency
	assert.Greater(t, buf.MaxE2ELatency, time.Duration(0))
	assert.Greater(t, buf.MinE2ELatency, time.Duration(0))

	// Request latency
	assert.Greater(t, buf.MaxRequestLatency, time.Duration(0))
	assert.Greater(t, buf.MinRequestLatency, time.Duration(0))

	// Transform latency
	assert.Greater(t, buf.MaxTransformLatency, time.Duration(0))
	assert.Greater(t, buf.MinTransformLatency, time.Duration(0))
}
