// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
func NewKinesisTarget(region string, streamName string) *KinesisTarget {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	return &KinesisTarget{
		Client:     kinesis.New(sess),
		StreamName: streamName,
	}
}

// Write pushes all events to the required target
func (kt *KinesisTarget) Write(event events.KinesisEvent) error {
	entries := make([]*kinesis.PutRecordsRequestEntry, len(event.Records))
	for i := 0; i < len(entries); i++ {
		record := event.Records[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         record.Kinesis.Data,
			PartitionKey: aws.String(record.Kinesis.PartitionKey),
		}
	}
	log.Debugf("Entries to write to target stream '%s': %v\n", kt.StreamName, entries)

	res, err := kt.Client.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(kt.StreamName),
	})
	if err != nil {
		return err
	}

	if *res.FailedRecordCount > int64(0) {
		return fmt.Errorf("Failed to write %d out of %d records to target stream '%s'", res.FailedRecordCount, len(entries), kt.StreamName)
	}

	return nil
}
