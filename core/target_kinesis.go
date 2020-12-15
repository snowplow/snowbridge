// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	log "github.com/sirupsen/logrus"
)

// KinesisTarget holds a new client for writing events to kinesis
type KinesisTarget struct {
	Client     kinesisiface.KinesisAPI
	StreamName string
}

// NewKinesisTarget creates a new client for writing events to kinesis
func NewKinesisTarget(region string, streamName string, roleARN string) *KinesisTarget {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	var kinesisClient kinesisiface.KinesisAPI
	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		kinesisClient = kinesis.New(sess, &aws.Config{
			Credentials: creds,
			Region: aws.String(region),
		})
	} else {
		kinesisClient = kinesis.New(sess)
	}

	return &KinesisTarget{
		Client:     kinesisClient,
		StreamName: streamName,
	}
}

// Write pushes all events to the required target
func (kt *KinesisTarget) Write(events []*Event) error {
	log.Infof("Writing %d records to target stream '%s' ...", len(events), kt.StreamName)

	entries := make([]*kinesis.PutRecordsRequestEntry, len(events))
	for i := 0; i < len(entries); i++ {
		event := events[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         event.Data,
			PartitionKey: aws.String(event.PartitionKey),
		}
	}

	log.Debugf("Entries (%d) to write to target stream '%s': %v\n", len(entries), kt.StreamName, entries)

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

	log.Infof("Successfully wrote %d records to target stream '%s'", len(entries), kt.StreamName)

	return nil
}
