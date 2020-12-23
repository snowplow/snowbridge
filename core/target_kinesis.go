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
)

// KinesisTarget holds a new client for writing events to kinesis
type KinesisTarget struct {
	Client     kinesisiface.KinesisAPI
	StreamName string
}

// NewKinesisTarget creates a new client for writing events to kinesis
func NewKinesisTarget(region string, streamName string, roleARN string) (*KinesisTarget, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	kinesisClient := kinesis.New(awsSession, awsConfig)

	return &KinesisTarget{
		Client:     kinesisClient,
		StreamName: streamName,
	}, nil
}

// Write pushes all events to the required target
// TODO: Add event batching (max: 500)
func (kt *KinesisTarget) Write(events []*Event) error {
	log.Debugf("Writing %d messages to Kinesis stream '%s' ...", len(events), kt.StreamName)

	entries := make([]*kinesis.PutRecordsRequestEntry, len(events))
	for i := 0; i < len(entries); i++ {
		event := events[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         event.Data,
			PartitionKey: aws.String(event.PartitionKey),
		}
	}

	log.Debugf("Entries (%d) to write to Kinesis stream '%s': %v\n", len(entries), kt.StreamName, entries)

	res, err := kt.Client.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(kt.StreamName),
	})
	if err != nil {
		return err
	}

	if *res.FailedRecordCount > int64(0) {
		return fmt.Errorf("Failed to write %d out of %d messages to Kinesis stream '%s'", res.FailedRecordCount, len(entries), kt.StreamName)
	}

	for _, event := range events {
		if event.AckFunc != nil {
			event.AckFunc()
		}
	}

	log.Debugf("Successfully wrote %d messages to Kinesis stream '%s'", len(entries), kt.StreamName)

	return nil
}

// Close does not do anything for this target
func (kt *KinesisTarget) Close() {}
