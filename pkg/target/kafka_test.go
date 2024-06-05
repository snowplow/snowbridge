/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package target

import (
	"sync/atomic"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/batchtransform"
	"github.com/snowplow/snowbridge/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func SetUpMockAsyncProducer(t *testing.T) (*mocks.AsyncProducer, *KafkaTarget) {
	config := mocks.NewTestConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	mp := mocks.NewAsyncProducer(t, config)

	asyncResults := make(chan *saramaResult)

	go func() {
		for err := range mp.Errors() {
			asyncResults <- &saramaResult{Msg: err.Msg, Err: err.Err}
		}
	}()

	go func() {
		for success := range mp.Successes() {
			asyncResults <- &saramaResult{Msg: success}
		}
	}()

	return mp, &KafkaTarget{
		syncProducer:     nil,
		asyncProducer:    mp,
		asyncResults:     asyncResults,
		messageByteLimit: 1048576,
		log:              log.WithFields(log.Fields{"target": "kafka"}),
	}
}

func SetUpMockSyncProducer(t *testing.T) (*mocks.SyncProducer, *KafkaTarget) {
	config := mocks.NewTestConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	mp := mocks.NewSyncProducer(t, config)

	return mp, &KafkaTarget{
		syncProducer:     mp,
		asyncProducer:    nil,
		asyncResults:     nil,
		messageByteLimit: 1048576,
		log:              log.WithFields(log.Fields{"target": "kafka"}),
	}
}

func TestKafkaTarget_AsyncWriteFailure(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockAsyncProducer(t)

	mockProducer.ExpectInputAndFail(sarama.ErrOutOfBrokers)

	defer target.Close()
	target.Open()

	messages := testutil.GetTestMessages(1, "Hello Kafka!!", nil)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error writing messages to Kafka topic: : 1 error occurred:\n\t* kafka: client has run out of available brokers to talk to\n\n", err.Error())
	}
	assert.NotNil(writeRes)

	// Check results
	assert.Equal(int64(0), writeRes.SentCount)
	assert.Equal(int64(1), writeRes.FailedCount)
}

func TestKafkaTarget_AsyncWriteSuccess(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockAsyncProducer(t)

	for i := 0; i < 501; i++ {
		mockProducer.ExpectInputAndSucceed()
	}

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(501, "Hello Kafka!!", ackFunc)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(501), ackOps)

	// Check results
	assert.Equal(int64(501), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestKafkaTarget_SyncWriteFailure(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockSyncProducer(t)

	mockProducer.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	defer target.Close()
	target.Open()

	messages := testutil.GetTestMessages(1, "Hello Kafka!!", nil)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error writing messages to Kafka topic: : 1 error occurred:\n\t* kafka: client has run out of available brokers to talk to\n\n", err.Error())
	}
	assert.NotNil(writeRes)

	// Check results
	assert.Equal(int64(0), writeRes.SentCount)
	assert.Equal(int64(1), writeRes.FailedCount)
}

func TestKafkaTarget_SyncWriteSuccess(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockSyncProducer(t)

	for i := 0; i < 501; i++ {
		mockProducer.ExpectSendMessageAndSucceed()
	}

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(501, "Hello Kafka!!", ackFunc)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(501), ackOps)

	// Check results
	assert.Equal(int64(501), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestKafkaTarget_WriteSuccess_OversizeBatch(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockAsyncProducer(t)

	for i := 0; i < 20; i++ {
		mockProducer.ExpectInputAndSucceed()
	}

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Kafka!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(10, testutil.GenRandomString(1048576), ackFunc)...)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(20), ackOps)

	// Check results
	assert.Equal(int64(20), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
}

func TestKafkaTarget_WriteSuccess_OversizeRecord(t *testing.T) {
	assert := assert.New(t)

	mockProducer, target := SetUpMockAsyncProducer(t)

	for i := 0; i < 10; i++ {
		mockProducer.ExpectInputAndSucceed()
	}

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Kafka!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(1, testutil.GenRandomString(1048577), ackFunc)...)

	batchTransform := batchtransform.NewBatchTransformation(nil)

	writeRes, err := target.Write(messages, batchTransform)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(10), ackOps)

	// Check results
	assert.Equal(int64(10), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
	assert.Equal(1, len(writeRes.Oversized))
}
