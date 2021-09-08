// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestSQSTarget_WriteFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	target, err := NewSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, "not-exists")
	assert.Nil(err)
	assert.NotNil(target)
	assert.Equal("arn:aws:sqs:us-east-1:00000000000:not-exists", target.GetID())

	res, err := target.Write(nil)
	assert.Nil(err)
	assert.NotNil(res)
}

func TestSQSTarget_WriteSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-1"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}
	queueURL := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	target, err := NewSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, queueName)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(100, "Hello SQS!!", ackFunc)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(100), ackOps)

	// Check results
	assert.Equal(int64(100), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestSQSTarget_WritePartialFailure_OversizeRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-2"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}
	queueURL := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	target, err := NewSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, queueName)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(100, "Hello SQS!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(1, testutil.GenRandomString(1048577), ackFunc)...)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(100), ackOps)

	// Check results
	assert.Equal(int64(100), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
	assert.Equal(1, len(writeRes.Oversized))
}
