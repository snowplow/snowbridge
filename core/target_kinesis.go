// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	// PutRecords API is limited to 500 messages in a single request
	kinesisPutRecordsChunkSize = 500
)

// KinesisTarget holds a new client for writing messages to kinesis
type KinesisTarget struct {
	Client     kinesisiface.KinesisAPI
	StreamName string
	log        *log.Entry
}

// NewKinesisTarget creates a new client for writing messages to kinesis
func NewKinesisTarget(region string, streamName string, roleARN string) (*KinesisTarget, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	kinesisClient := kinesis.New(awsSession, awsConfig)

	return &KinesisTarget{
		Client:     kinesisClient,
		StreamName: streamName,
		log:        log.WithFields(log.Fields{"name": "KinesisTarget"}),
	}, nil
}

// Write pushes all messages to the required target
// TODO: Should each put be in its own goroutine?
func (kt *KinesisTarget) Write(messages []*Message) (*TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to stream '%s' ...", len(messages), kt.StreamName)

	sent := int64(0)
	failed := int64(0)
	var errstrings []string

	messagesChunked := getChunkedMessages(messages, kinesisPutRecordsChunkSize)
	for _, messageChunk := range messagesChunked {
		res, err := kt.process(messageChunk)

		if res != nil {
			sent += res.Sent
			failed += res.Failed
		}
		if err != nil {
			errstrings = append(errstrings, err.Error())
		}
	}

	var err error
	if len(errstrings) > 0 {
		err = fmt.Errorf(strings.Join(errstrings, "\n"))
	}

	kt.log.Debugf("Successfully wrote %d/%d messages to stream '%s'", sent, len(messages), kt.StreamName)
	return NewWriteResult(sent, failed, messages), err
}

func (kt *KinesisTarget) process(messages []*Message) (*TargetWriteResult, error) {
	kt.log.Debugf("Writing chunk of %d messages to stream '%s' ...", len(messages), kt.StreamName)

	entries := make([]*kinesis.PutRecordsRequestEntry, len(messages))
	for i := 0; i < len(entries); i++ {
		msg := messages[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         msg.Data,
			PartitionKey: aws.String(msg.PartitionKey),
		}
	}

	kt.log.Debugf("Entries (%d) to write to stream '%s': %v\n", len(entries), kt.StreamName, entries)

	res, err := kt.Client.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(kt.StreamName),
	})
	if err != nil {
		return nil, err
	}

	// TODO: Can we ack successful messages when some fail in the batch? This will cause duplicate processing on failure.
	if res.FailedRecordCount != nil && *res.FailedRecordCount > int64(0) {
		return &TargetWriteResult{
			Sent:   int64(len(messages)) - *res.FailedRecordCount,
			Failed: *res.FailedRecordCount,
		}, fmt.Errorf("Failed to write %d/%d messages to stream '%s'", res.FailedRecordCount, len(entries), kt.StreamName)
	}

	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	kt.log.Debugf("Successfully wrote %d messages to stream '%s'", len(entries), kt.StreamName)

	return &TargetWriteResult{
		Sent:   int64(len(messages)),
		Failed: int64(0),
	}, nil
}

// Open does not do anything for this target
func (kt *KinesisTarget) Open() {}

// Close does not do anything for this target
func (kt *KinesisTarget) Close() {}
