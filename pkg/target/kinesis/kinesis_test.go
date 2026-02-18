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

package kinesis

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

// newKinesisTargetWithInterfaces creates a Kinesis target with mocked interfaces for testing
func newKinesisTargetWithInterfaces(client common.KinesisV2API, accountID, region, streamName string, requestMaxMessages int) (*KinesisTargetDriver, error) {
	return &KinesisTargetDriver{
		BatchingConfig: targetiface.BatchingConfig{
			MaxBatchMessages:     requestMaxMessages,
			MaxBatchBytes:        kinesisPutRecordsRequestByteLimit,
			MaxMessageBytes:      kinesisPutRecordsMessageByteLimit,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
		client:     client,
		streamName: streamName,
		region:     region,
		accountID:  accountID,
		log:        logrus.WithFields(logrus.Fields{"target": SupportedTargetKinesis, "cloud": "AWS", "region": region, "stream": streamName}),
	}, nil
}

func TestBuildKinesisFromConfig_InvalidRequestMaxMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	if err := os.Setenv("AWS_ACCESS_KEY_ID", "foo"); err != nil {
		t.Fatalf("failed to set AWS_ACCESS_KEY_ID var: %s", err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "bar"); err != nil {
		t.Fatalf("failed to set AWS_SECRET_ACCESS_KEY var: %s", err)
	}

	cfg := &KinesisTargetConfig{
		StreamName:        "test-stream",
		Region:            testutil.AWSLocalstackRegion,
		CustomAWSEndpoint: testutil.AWSLocalstackEndpoint,
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages: 600,
		},
	}

	target := &KinesisTargetDriver{}

	err := target.InitFromConfig(cfg)
	assert.NotNil(err)
	assert.Contains(err.Error(), "request_max_messages cannot be higher than the Kinesis PutRecords limit of 500")
}

func TestKinesisTargetDriver_Batcher(t *testing.T) {
	driver := &KinesisTargetDriver{}
	defaultConfig := driver.GetDefaultConfiguration().(*KinesisTargetConfig)
	driver.BatchingConfig = *defaultConfig.BatchingConfig

	// Test 1: Adding one message to a batch with 499 messages should trigger send
	// Create a current batch with 499 messages (one less than Kinesis's default max of 500)
	smallMessages := testutil.GetTestMessages(499, "test", nil)
	currentBatchDataBytes := 0
	for _, msg := range smallMessages {
		currentBatchDataBytes += len(msg.Data)
	}

	currentBatch := targetiface.CurrentBatch{
		Messages:  smallMessages,
		DataBytes: currentBatchDataBytes,
	}

	// Add one more message (the 500th)
	additionalMessage := testutil.GetTestMessages(1, "test", nil)[0]

	batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, additionalMessage)

	// Verify complete batch is sent (500 messages - Kinesis's default max)
	assert.Len(t, batchToSend, 500, "Should send complete batch of 500 messages")

	// Verify new current batch is empty
	assert.Len(t, newCurrentBatch.Messages, 0, "Should have empty current batch after sending")
	assert.Equal(t, 0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")

	// Verify no oversized message
	assert.Nil(t, oversized, "Should have no oversized message")

	// Test 2: Oversized message should be returned as oversized
	// Create an oversized message (larger than 1MB)
	oversizedMessage := testutil.GetTestMessages(1, testutil.GenRandomString(1100000), nil)[0]

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
}

func TestKinesisTarget_WriteFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	if err := os.Setenv("AWS_ACCESS_KEY_ID", "foo"); err != nil {
		t.Fatalf("failed to set AWS_ACCESS_KEY_ID var: %s", err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "bar"); err != nil {
		t.Fatalf("failed to set AWS_SECRET_ACCESS_KEY var: %s", err)
	}

	client := testutil.GetAWSLocalstackKinesisClient()

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, "not-exists", 500)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	err = target.Open()
	assert.Nil(err)

	messages := testutil.GetTestMessages(1, "Hello Kinesis!!", nil)

	writeRes, err := target.Write(messages)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "not found")
	}
	assert.NotNil(writeRes)

	// Check results
	assert.Equal(0, len(writeRes.Sent))
	assert.Equal(1, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Oversized))
	assert.Equal(0, len(writeRes.Invalid))
}

func TestKinesisTarget_WriteSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	if err := os.Setenv("AWS_ACCESS_KEY_ID", "foo"); err != nil {
		t.Fatalf("failed to set AWS_ACCESS_KEY_ID var: %s", err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "bar"); err != nil {
		t.Fatalf("failed to set AWS_SECRET_ACCESS_KEY var: %s", err)
	}

	client := testutil.GetAWSLocalstackKinesisClient()

	streamName := "kinesis-stream-target-1"
	err := testutil.CreateAWSLocalstackKinesisStream(client, streamName, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(client, streamName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, streamName, 500)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	err = target.Open()
	assert.Nil(err)

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(500, "Hello Kinesis!!", ackFunc)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(500), ackOps)

	// Check results
	assert.Equal(500, len(writeRes.Sent))
	assert.Equal(0, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Oversized))
	assert.Equal(0, len(writeRes.Invalid))
}
