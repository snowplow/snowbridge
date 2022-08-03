// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

// TestNewSqsTarget_AWSConnectionCheck tests that the SQS target fails on start-up if the connection to AWS fails
func TestNewSqsTarget_AWSConnectionCheck(t *testing.T) {
	assert := assert.New(t)

	target, err := newSQSTarget(testutil.AWSLocalstackRegion, "not-exists", `arn:aws:sqs:us-east-1:00000000000:not-exists`)
	assert.Nil(target)
	assert.EqualError(err, "NoCredentialProviders: no valid providers in chain. Deprecated.\n\tFor verbose messaging see aws.Config.CredentialsChainVerboseErrors")
}

func TestSQSTarget_SQSConnectionFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	target, err := newSQSTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, "not-exists")
	assert.Nil(target)
	assert.NotNil(err)
	if err != nil {
		assert.True(strings.HasPrefix(err.Error(), `Could not connect to SQS`))
	}
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
		t.Fatal(err)
	}
	queueURL := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

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

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-target-2"
	queueRes, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		t.Fatal(err)
	}
	queueURL := queueRes.QueueUrl
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

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
