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

package kinesissource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

// --- Test StatsReceiver for testing kinsumer metrics

type TestStatsReceiver struct {
	onSend func(b *models.ObserverBuffer)
}

func (s *TestStatsReceiver) Send(b *models.ObserverBuffer) {
	s.onSend(b)
}

// TODO: When we address https://github.com/snowplow/snowbridge/issues/151, this test will need to change.
func TestKinesisSource_ReadFailure_NoResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	cfg := DefaultConfiguration()
	cfg.StreamName = "not-exists"
	cfg.AppName = "fake-name"
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 1)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		// Source should log error and return cleanly when table doesn't exist
		source.Start(ctx)
	})

	// Source should exit quickly when it encounters the error
	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source should exit cleanly on error")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

func TestKinesisSource_ReadMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-2"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	appName := "integration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer func() {
		if err := testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put ten records into kinesis stream
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Test")
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second)

	// Create the source and assert that it's there
	cfg := DefaultConfiguration()
	cfg.StreamName = streamName
	cfg.AppName = appName
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(10, len(successfulReads))

	cancel()

	for _, msg := range successfulReads {
		assert.Contains(string(msg.Data), "Test")
		msg.AckFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

func TestKinesisSource_KinsumerMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Set log level to reduce noise
	logrus.SetLevel(logrus.WarnLevel)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-metrics-integration"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	appName := "integration-metrics"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer func() {
		if err := testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put ten records into kinesis stream
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Test")
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second)

	// Create the source and assert that it's there
	cfg := DefaultConfiguration()
	cfg.StreamName = streamName
	cfg.AppName = appName
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	// Set up observer with mock stats receiver to capture kinsumer metrics
	var capturedBuffers []*models.ObserverBuffer
	mockStatsReceiver := &TestStatsReceiver{
		onSend: func(b *models.ObserverBuffer) {
			capturedBuffers = append(capturedBuffers, b)
		},
	}
	obs := observer.New(mockStatsReceiver, 1*time.Second, 500*time.Millisecond, nil)

	source, err := BuildFromConfig(&cfg, obs)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	obs.Start()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	defer obs.Stop()

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(10, len(successfulReads))

	for _, msg := range successfulReads {
		// Sleep to allow kinsumer to accumulate records in memory while observer flushes metrics
		time.Sleep(2 * time.Second)
		msg.AckFunc()
	}

	// Verify that kinsumer metrics were captured in observer buffer
	assert.NotEmpty(capturedBuffers, "Observer buffers should have been captured during processing")

	// Find any buffer with non-zero kinsumer metrics
	foundNonZeroRecords := false
	foundNonZeroBytes := false
	for _, buffer := range capturedBuffers {
		if buffer.KinsumerRecordsInMemory > 0 {
			foundNonZeroRecords = true
		}
		if buffer.KinsumerRecordsInMemoryBytes > 0 {
			foundNonZeroBytes = true
		}
	}
	assert.True(foundNonZeroRecords, "Should have captured non-zero KinsumerRecordsInMemory during processing")
	assert.True(foundNonZeroBytes, "Should have captured non-zero KinsumerRecordsInMemoryBytes during processing")

	cancel()
	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

func TestKinesisSource_StartTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-3"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	appName := "integration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}

	defer func() {
		if err := testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put two batches of 10 records into kinesis stream, grabbing a timestamp in between
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "First batch")
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second) // Put a 1s buffer either side of the start timestamp
	timeToStart := time.Now().UTC()
	time.Sleep(1 * time.Second)

	putErr2 := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Second batch")
	if putErr2 != nil {
		t.Fatal(putErr2)
	}

	// Create the source (with start timestamp) and assert that it's there
	cfg := DefaultConfiguration()
	cfg.StreamName = streamName
	cfg.AppName = appName
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint
	cfg.StartTimestamp = timeToStart.Format("2006-01-02 15:04:05.999")

	source, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(10, len(successfulReads))

	cancel()

	for _, msg := range successfulReads {
		assert.Contains(string(msg.Data), "Second batch")
		msg.AckFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

// TestKinesisSource_WaitForDelayedAcks verifies that:
// 1. When context is cancelled, the source waits for all in-flight messages to be acked before shutting down
// 2. Messages can be acked out of order (last 5 before first 5) without blocking the main thread
// 3. Kinsumer handles out-of-order acking correctly while ensuring sequential checkpointing to DynamoDB
func TestKinesisSource_WaitForDelayedAcks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-delayed-acks"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	appName := "integration-delayed"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer func() {
		if err := testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put ten records into kinesis stream
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Test message")
	if putErr != nil {
		t.Fatal(putErr)
	}

	// Create the source
	cfg := DefaultConfiguration()
	cfg.StreamName = streamName
	cfg.AppName = appName
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)
	assert.Equal(10, len(successfulReads))

	// Cancel source first without acking anything yet
	cancel()

	assert.False(common.WaitWithTimeout(&wg, 5*time.Second), "Source finished even though it still waits for acks")

	// Now ack all messages but not in the receiving order.
	// However, this doesn't mean that the last 5 records are checkpointed (to DynamoDB) before the first 5.
	// It just means we don't block the main thread when calling the ack function.
	// Kinsumer ensures that the actual under-the-hood checkpointing happens in the correct order.
	for _, msg := range successfulReads[5:10] {
		msg.AckFunc()
	}

	for _, msg := range successfulReads[0:5] {
		msg.AckFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 5*time.Second), "Source is not finished even though it has been stopped and all messages have been acked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

// TestKinesisSource_SourceRestart verifies that:
// 1. When a source is restarted after processing and acking messages, it handles the restart correctly
// 2. The source processes new messages published after the first run
// 3. Messages that were not acked in the first run are redelivered and can be processed again
func TestKinesisSource_SourceRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-restart"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	appName := "integration-restart"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer func() {
		if err := testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put ten records into kinesis stream
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "msg-first-batch")
	if putErr != nil {
		t.Fatal(putErr)
	}

	// Create the source
	cfg := DefaultConfiguration()
	cfg.StreamName = streamName
	cfg.AppName = appName
	cfg.ClientName = "test_client_name"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)
	assert.Equal(10, len(successfulReads))

	cancel()

	// Ack only first 5 messages
	for _, msg := range successfulReads[0:5] {
		msg.AckFunc()
	}

	// Try to ack messages 8-9, skipping 5-7.
	// This shouldn't block or actually checkpoint any record from 8-9 (because of the gap at 5-7).
	for _, msg := range successfulReads[8:10] {
		msg.AckFunc()
	}

	// Kinsumer waits some time for the rest of acks (never happens), but should eventually quit.
	assert.True(common.WaitWithTimeout(&wg, 60*time.Second))

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")

	// Second batch! Publish new messages
	putErr = testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "msg-second-batch")
	if putErr != nil {
		t.Fatal(putErr)
	}

	// Build another source (simulating app restart) and confirm it consumes both nacked messages and new messages
	secondSource, err := BuildFromConfig(&cfg, nil)
	assert.Nil(err)
	assert.NotNil(secondSource)

	outputChannel = make(chan *models.Message, 20)

	secondSource.SetChannels(outputChannel)

	ctx, cancel = context.WithCancel(context.Background())
	wg.Go(func() {
		secondSource.Start(ctx)
	})

	successfulReads = testutil.ReadSourceOutput(outputChannel)

	// We should have 5 unacked from the first batch + 10 from the second batch = 15
	assert.Equal(15, len(successfulReads))

	cancel()

	for _, msg := range successfulReads {
		msg.AckFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok = <-outputChannel
	assert.False(ok, "Output channel should be closed")
}
