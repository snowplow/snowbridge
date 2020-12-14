// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/internal/common"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

const (
	// API Documentation: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html

	// Limited to 500 messages in a single request
	kinesisPutRecordsChunkSize = 500
	// Each record can only be up to 1 MiB in size
	kinesisPutRecordsMessageByteLimit = 1048576
	// Each request can be a maximum of 5 MiB in size total
	kinesisPutRecordsRequestByteLimit = kinesisPutRecordsMessageByteLimit * 5
)

// KinesisTarget holds a new client for writing messages to kinesis
type KinesisTarget struct {
	client     kinesisiface.KinesisAPI
	streamName string

	log *log.Entry
}

// NewKinesisTarget creates a new client for writing messages to kinesis
func NewKinesisTarget(region string, streamName string, roleARN string) (*KinesisTarget, error) {
	awsSession, awsConfig := common.GetAWSSession(region, roleARN)
	kinesisClient := kinesis.New(awsSession, awsConfig)

	return NewKinesisTargetWithInterfaces(kinesisClient, region, streamName)
}

// NewKinesisTargetWithInterfaces allows you to provide a Kinesis client directly to allow
// for mocking and localstack usage
func NewKinesisTargetWithInterfaces(client kinesisiface.KinesisAPI, region string, streamName string) (*KinesisTarget, error) {
	return &KinesisTarget{
		client:     client,
		streamName: streamName,
		log:        log.WithFields(log.Fields{"target": "kinesis", "cloud": "AWS", "region": region, "stream": streamName}),
	}, nil
}

// Write pushes all messages to the required target
// TODO: Should each put be in its own goroutine?
// TODO: How should oversized records be handled?
func (kt *KinesisTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to stream ...", len(messages))

	sent := int64(0)
	failed := int64(0)

	var errResult error

	messagesChunked := models.GetChunkedMessages(messages,
		kinesisPutRecordsChunkSize,
		kinesisPutRecordsMessageByteLimit,
		kinesisPutRecordsRequestByteLimit)

	for _, messageChunk := range messagesChunked {
		res, err := kt.process(messageChunk)

		if res != nil {
			sent += res.Sent
			failed += res.Failed
		}

		if err != nil {
			errResult = multierror.Append(errResult, err)
			failed += int64(len(messageChunk))
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to Kinesis stream")
	}

	kt.log.Debugf("Successfully wrote %d/%d messages", sent, len(messages))
	return models.NewWriteResult(sent, failed, messages), errResult
}

func (kt *KinesisTarget) process(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing chunk of %d messages to stream ...", len(messages))

	entries := make([]*kinesis.PutRecordsRequestEntry, len(messages))
	for i := 0; i < len(entries); i++ {
		msg := messages[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         msg.Data,
			PartitionKey: aws.String(msg.PartitionKey),
		}
	}

	res, err := kt.client.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(kt.streamName),
	})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to send message batch to Kinesis stream")
	}

	// TODO: Can we ack successful messages when some fail in the batch? This will cause duplicate processing on failure.
	if res.FailedRecordCount != nil && *res.FailedRecordCount > int64(0) {
		return &models.TargetWriteResult{
			Sent:   int64(len(messages)) - *res.FailedRecordCount,
			Failed: *res.FailedRecordCount,
		}, errors.New("Failed to write all messages in batch to Kinesis stream")
	}

	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	kt.log.Debugf("Successfully wrote %d messages", len(entries))

	return &models.TargetWriteResult{
		Sent:   int64(len(messages)),
		Failed: int64(0),
	}, nil
}

// Open does not do anything for this target
func (kt *KinesisTarget) Open() {}

// Close does not do anything for this target
func (kt *KinesisTarget) Close() {}
