//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package target

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestKinesisTarget_WriteFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackKinesisClient()

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, "not-exists")
	assert.Nil(err)
	assert.NotNil(target)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/not-exists", target.GetID())

	defer target.Close()
	target.Open()

	messages := testutil.GetTestMessages(1, "Hello Kinesis!!", nil)

	writeRes, err := target.Write(messages)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "Error writing messages to Kinesis stream: 1 error occurred:")
	}
	assert.NotNil(writeRes)

	// Check results
	assert.Equal(int64(0), writeRes.SentCount)
	assert.Equal(int64(1), writeRes.FailedCount)
}

func TestKinesisTarget_WriteSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackKinesisClient()

	streamName := "kinesis-stream-target-1"
	err := testutil.CreateAWSLocalstackKinesisStream(client, streamName, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(client, streamName)

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, streamName)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(501, "Hello Kinesis!!", ackFunc)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(501), ackOps)

	// Check results
	assert.Equal(int64(501), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestKinesisTarget_WriteSuccess_OversizeBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackKinesisClient()

	streamName := "kinesis-stream-target-2"
	err := testutil.CreateAWSLocalstackKinesisStream(client, streamName, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(client, streamName)

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, streamName)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Kinesis!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(10, testutil.GenRandomString(1048576), ackFunc)...)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(20), ackOps)

	// Check results
	assert.Equal(int64(20), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestKinesisTarget_WriteSuccess_OversizeRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackKinesisClient()

	streamName := "kinesis-stream-target-3"
	err := testutil.CreateAWSLocalstackKinesisStream(client, streamName, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(client, streamName)

	target, err := newKinesisTargetWithInterfaces(client, "00000000000", testutil.AWSLocalstackRegion, streamName)
	assert.Nil(err)
	assert.NotNil(target)

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Kinesis!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(1, testutil.GenRandomString(1048577), ackFunc)...)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(10), ackOps)

	// Check results
	assert.Equal(int64(10), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
	assert.Equal(1, len(writeRes.Oversized))
}
