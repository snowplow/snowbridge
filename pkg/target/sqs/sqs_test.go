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

package sqs

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestSQSTarget_OpenFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	driver, err := newLocalstackSQSDriver("not-exists")
	assert.Nil(err)
	assert.NotNil(driver)

	err = driver.Open()
	assert.NotNil(err)
	assert.Contains(err.Error(), "Failed to get SQS queue URL")
}

func TestSQSTarget_WriteSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-1"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueRes.QueueUrl
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(client, queueURL); err != nil {
			logrus.Error(err.Error())
		}
	}()

	driver, err := newLocalstackSQSDriver(queueName)
	assert.Nil(err)
	assert.NotNil(driver)

	defer driver.Close()
	err = driver.Open()
	assert.Nil(err)

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello SQS!!", ackFunc)

	writeRes, err := driver.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(10), ackOps)

	// Check results
	assert.Equal(10, len(writeRes.Sent))
	assert.Equal(0, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Invalid))
}

func TestSQSTargetDriver_Batcher(t *testing.T) {
	driver := &SQSTargetDriver{}
	defaultConfig := driver.GetDefaultConfiguration().(*SQSTargetConfig)
	driver.BatchingConfig = *defaultConfig.BatchingConfig

	t.Run("adding 10th message triggers send with empty new batch", func(t *testing.T) {
		smallMessages := testutil.GetTestMessages(9, "test", nil)
		currentBatchDataBytes := 0
		for _, msg := range smallMessages {
			currentBatchDataBytes += len(msg.Data)
		}

		currentBatch := targetiface.CurrentBatch{
			Messages:  smallMessages,
			DataBytes: currentBatchDataBytes,
		}

		// Add one more message (the 10th)
		additionalMessage := testutil.GetTestMessages(1, "test", nil)[0]

		batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, additionalMessage)

		// Verify complete batch is sent (10 messages - SQS's default max)
		assert.Len(t, batchToSend, 10, "Should send complete batch of 10 messages")

		// Verify new current batch is empty
		assert.Len(t, newCurrentBatch.Messages, 0, "Should have empty current batch after sending")
		assert.Equal(t, 0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")

		// Verify no oversized message
		assert.Nil(t, oversized, "Should have no oversized message")
	})

	t.Run("oversized message is returned as oversized with no batch sent", func(t *testing.T) {
		oversizedMessage := testutil.GetTestMessages(1, testutil.GenRandomString(1_049_576), nil)[0]

		// Start with empty batch for oversized test
		emptyBatch := targetiface.CurrentBatch{}

		batchToSend2, newCurrentBatch2, oversized2 := driver.Batcher(emptyBatch, oversizedMessage)

		// Verify no batch is sent
		assert.Nil(t, batchToSend2, "Should not send any batch for oversized message")

		// Verify current batch remains empty
		assert.Len(t, newCurrentBatch2.Messages, 0, "Current batch should remain empty")
		assert.Equal(t, 0, newCurrentBatch2.DataBytes, "Current batch bytes should remain 0")

		// Verify oversized message is returned
		assert.NotNil(t, oversized2, "Should return oversized message")
		assert.Equal(t, oversizedMessage, oversized2, "Should return the exact oversized message")
	})
}

// newLocalstackSQSDriver creates an SQS driver targeting localstack
func newLocalstackSQSDriver(queueName string) (*SQSTargetDriver, error) {
	driver := &SQSTargetDriver{}

	c := driver.GetDefaultConfiguration()
	cfg, ok := c.(*SQSTargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type")
	}

	cfg.QueueName = queueName
	cfg.Region = testutil.AWSLocalstackRegion
	cfg.CustomAWSEndpoint = testutil.AWSLocalstackEndpoint

	err := driver.InitFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return driver, nil
}
