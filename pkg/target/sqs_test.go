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

package target

import (
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestSQSTarget_WriteFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClientV2()

	target, err := newSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, "not-exists")
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

	client := testutil.GetAWSLocalstackSQSClientV2()

	queueName := "sqs-queue-target-1"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueueV2(client, queueName)
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueRes.QueueUrl
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueueV2(client, queueURL); err != nil {
			logrus.Error(err.Error())
		}
	}()

	target, err := newSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, queueName)
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

	client := testutil.GetAWSLocalstackSQSClientV2()

	queueName := "sqs-queue-target-2"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueueV2(client, queueName)
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueRes.QueueUrl
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueueV2(client, queueURL); err != nil {
			logrus.Error(err.Error())
		}
	}()

	target, err := newSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, queueName)
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
