// +build integration

// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestSQSTarget_WriteFailure(t *testing.T) {
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	target, err := NewSQSTargetWithInterfaces(client, testutil.AWSLocalstackRegion, "not-exists")
	assert.Nil(err)
	assert.NotNil(target)

	res, err := target.Write(nil)
	assert.NotNil(err)
	assert.NotNil(res)
}

func TestSQSTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-1"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}
	queueUrl := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueUrl)

	target, err := NewSQSTargetWithInterfaces(client, testutil.AWSLocalstackRegion, queueName)
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
	assert.Equal(int64(100), writeRes.Sent)
	assert.Equal(int64(0), writeRes.Failed)
}

func TestSQSTarget_WritePartialFailure_OversizeRecord(t *testing.T) {
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-2"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}
	queueUrl := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueUrl)

	target, err := NewSQSTargetWithInterfaces(client, testutil.AWSLocalstackRegion, queueName)
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
	assert.NotNil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(100), ackOps)

	// Check results
	assert.Equal(int64(100), writeRes.Sent)
	assert.Equal(int64(1), writeRes.Failed)
}
