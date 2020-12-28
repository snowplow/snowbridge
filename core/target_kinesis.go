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

// KinesisTarget holds a new client for writing events to kinesis
type KinesisTarget struct {
	Client     kinesisiface.KinesisAPI
	StreamName string
	log        *log.Entry
}

// NewKinesisTarget creates a new client for writing events to kinesis
func NewKinesisTarget(region string, streamName string, roleARN string) (*KinesisTarget, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	kinesisClient := kinesis.New(awsSession, awsConfig)

	return &KinesisTarget{
		Client:     kinesisClient,
		StreamName: streamName,
		log:        log.WithFields(log.Fields{"name": "KinesisTarget"}),
	}, nil
}

// Write pushes all events to the required target
// TODO: Should each put be in its own goroutine?
func (kt *KinesisTarget) Write(events []*Event) (*WriteResult, error) {
	kt.log.Debugf("Writing %d messages to stream '%s' ...", len(events), kt.StreamName)

	sent := int64(0)
	failed := int64(0)
	var errstrings []string

	eventsChunked := getChunkedEvents(events, kinesisPutRecordsChunkSize)
	for _, eventsChunk := range eventsChunked {
		res, err := kt.process(eventsChunk)

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

	kt.log.Debugf("Successfully wrote %d/%d messages to stream '%s'", sent, len(events), kt.StreamName)

	return &WriteResult{
		Sent:   sent,
		Failed: failed,
	}, err
}

func (kt *KinesisTarget) process(events []*Event) (*WriteResult, error) {
	kt.log.Debugf("Writing chunk of %d messages to stream '%s' ...", len(events), kt.StreamName)

	entries := make([]*kinesis.PutRecordsRequestEntry, len(events))
	for i := 0; i < len(entries); i++ {
		event := events[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         event.Data,
			PartitionKey: aws.String(event.PartitionKey),
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

	// TODO: Can we ack successful events when some fail in the batch? This will cause duplicate processing on failure.
	if res.FailedRecordCount != nil && *res.FailedRecordCount > int64(0) {
		return &WriteResult{
			Sent:   int64(len(events)) - *res.FailedRecordCount,
			Failed: *res.FailedRecordCount,
		}, fmt.Errorf("Failed to write %d/%d messages to stream '%s'", res.FailedRecordCount, len(entries), kt.StreamName)
	}

	for _, event := range events {
		if event.AckFunc != nil {
			event.AckFunc()
		}
	}

	kt.log.Debugf("Successfully wrote %d messages to stream '%s'", len(entries), kt.StreamName)

	return &WriteResult{
		Sent:   int64(len(events)),
		Failed: int64(0),
	}, nil
}

// Close does not do anything for this target
func (kt *KinesisTarget) Close() {}
