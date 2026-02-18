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

package sqssource

import (
	"context"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestBuildFromConfig_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 50, "Hello SQS!!")
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(client, queueURL); err != nil {
			log.Error(err.Error())
		}
	}()

	cfg := DefaultConfiguration()
	cfg.QueueName = queueName
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg)

	assert.Nil(err)
	assert.NotNil(source)
}

func TestBuildFromConfig_SetupFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	cfg := DefaultConfiguration()
	cfg.QueueName = "not-exists"
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	_, err := BuildFromConfig(&cfg)
	assert.NotNil(err)
	assert.ErrorContains(err, "Failed to get SQS queue URL:")
}

func TestSQSSource_StartSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source-read"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 50, "Hello SQS!!")
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(client, queueURL); err != nil {
			log.Error(err.Error())
		}
	}()

	// Create the source using BuildFromConfig
	cfg := DefaultConfiguration()
	cfg.QueueName = queueName
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 50)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(50, len(successfulReads))

	for _, msg := range successfulReads {
		assert.Equal("Hello SQS!!", string(msg.Data))
		assert.Greater(msg.TimePulled, msg.TimeCreated)
	}

	cancel()

	// Ack all messages
	for _, msg := range successfulReads {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	assert.True(common.WaitWithTimeout(&wg, 5*time.Second), "Source is not finished even though it has been stopped and all messages have been acked")
}

// TestSQSSource_SourceRestart verifies that:
// 1. When a source is restarted after processing and acking/nacking messages, it handles the restart correctly
// 2. The source processes new messages published after the first run
// 3. Messages that were nacked in the first run are redelivered and can be processed again
func TestSQSSource_SourceRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source-restart"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 10, "msg-first-batch")
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(client, queueURL); err != nil {
			log.Error(err.Error())
		}
	}()

	// Create the source using BuildFromConfig
	cfg := DefaultConfiguration()
	cfg.QueueName = queueName
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	source, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 100)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)
	assert.Equal(10, len(successfulReads))

	cancel()

	// Source has been cancelled and finished processing, it won't produce more messages
	assert.True(common.WaitWithTimeout(&wg, 1*time.Second))

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")

	// Even though source has been stopped, we can still ack/nack what we've pulled so far.
	// For SQS we don't need its Start(ctx) function to keep running.
	// Ack 5 messages from the first batch...
	for _, msg := range successfulReads[0:5] {
		msg.AckFunc()
	}

	// And nack the other 5 messages...
	for _, msg := range successfulReads[5:10] {
		msg.NackFunc()
	}

	// Second batch! Publish new messages
	secondBatchMessages := make([]string, 10)
	for i := range 10 {
		secondBatchMessages[i] = "msg-second-batch"
	}
	testutil.PutProvidedDataIntoSQS(client, *queueURL, secondBatchMessages)

	// Build another source (simulating app restart) and confirm it consumes both nacked messages and new messages
	secondSource, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(secondSource)

	outputChannel = make(chan *models.Message, 15)

	secondSource.SetChannels(outputChannel)

	ctx, cancel = context.WithCancel(context.Background())
	wg.Go(func() {
		secondSource.Start(ctx)
	})

	// Eventually we should have 5 nacked from the first batch + 10 from the second batch, so 15 total
	successfulReads = make([]*models.Message, 0)
	for i := 0; i < 5; i++ {
		successfulReads = append(successfulReads, testutil.ReadSourceOutput(outputChannel)...)
		for _, msg := range successfulReads {
			msg.AckFunc()
		}
		if len(successfulReads) > 14 {
			break
		}
	}
	assert.Equal(15, len(successfulReads))

	cancel()

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked")

	_, ok = <-outputChannel
	assert.False(ok, "Output channel should be closed")
}
