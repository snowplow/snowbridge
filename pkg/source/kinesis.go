// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package source

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/twitchscience/kinsumer"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// --- Kinsumer overrides

// KinsumerLogrus adds a Logrus logger for Kinsumer
type KinsumerLogrus struct{}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *KinsumerLogrus) Log(format string, v ...interface{}) {
	log.WithFields(log.Fields{"source": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// --- Kinesis source

// KinesisSource holds a new client for reading messages from kinesis
type KinesisSource struct {
	client           *kinsumer.Kinsumer
	streamName       string
	concurrentWrites int
	region           string
	accountID        string

	log *log.Entry
}

// NewKinesisSource creates a new client for reading messages from kinesis
func NewKinesisSource(concurrentWrites int, region string, streamName string, roleARN string, appName string) (*KinesisSource, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(region, roleARN)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.New(awsSession, awsConfig)
	dynamodbClient := dynamodb.New(awsSession, awsConfig)

	return NewKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, *awsAccountID, concurrentWrites, region, streamName, appName)
}

// NewKinesisSourceWithInterfaces allows you to provide a Kinesis + DynamoDB client directly to allow
// for mocking and localstack usage
func NewKinesisSourceWithInterfaces(kinesisClient kinesisiface.KinesisAPI, dynamodbClient dynamodbiface.DynamoDBAPI, awsAccountID string, concurrentWrites int, region string, streamName string, appName string) (*KinesisSource, error) {
	// TODO: Add statistics monitoring to be able to report on consumer latency
	config := kinsumer.NewConfig().
		WithShardCheckFrequency(10 * time.Second).
		WithLeaderActionFrequency(10 * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{})

	// TODO: See if the client name can be reused to survive same node reboots
	name := uuid.NewV4().String()

	k, err := kinsumer.NewWithInterfaces(kinesisClient, dynamodbClient, streamName, appName, name, config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kinsumer client")
	}

	return &KinesisSource{
		client:           k,
		streamName:       streamName,
		concurrentWrites: concurrentWrites,
		region:           region,
		accountID:        awsAccountID,
		log:              log.WithFields(log.Fields{"source": "kinesis", "cloud": "AWS", "region": region, "stream": streamName}),
	}, nil
}

// Read will pull messages from the noted Kinesis stream forever
func (ks *KinesisSource) Read(sf *sourceiface.SourceFunctions) error {
	ks.log.Infof("Reading messages from stream ...")

	err := ks.client.Run()
	if err != nil {
		return errors.Wrap(err, "Failed to start Kinsumer client")
	}

	throttle := make(chan struct{}, ks.concurrentWrites)
	wg := sync.WaitGroup{}

	for {
		record, checkpointer, err := ks.client.NextRecordWithCheckpointer()
		if err != nil {
			return errors.Wrap(err, "Failed to pull next Kinesis record from Kinsumer client")
		}

		timePulled := time.Now().UTC()

		ackFunc := func() {
			ks.log.Debugf("Ack'ing record with SequenceNumber: %s", *record.SequenceNumber)
			checkpointer()
		}

		if record != nil {
			timeCreated := record.ApproximateArrivalTimestamp.UTC()
			messages := []*models.Message{
				{
					Data:         record.Data,
					PartitionKey: *record.PartitionKey,
					AckFunc:      ackFunc,
					TimeCreated:  timeCreated,
					TimePulled:   timePulled,
				},
			}

			throttle <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sf.WriteToTarget(messages)

				// The Kinsumer client blocks unless we can checkpoint which only happens
				// on a successful write to the target.  As such we need to force an app
				// close in this scenario to allow it to reboot and hopefully continue.
				if err != nil {
					ks.log.WithFields(log.Fields{"error": err}).Fatal(err)
				}
				<-throttle
			}()
		} else {
			ks.log.Info("BETA TEST DEBUG: Returning nil: record = nil")
			return nil
		}
	}
	ks.log.Info("BETA TEST DEBUG: Unreachable code reached somehow.")
	wg.Wait()

	return nil
}

// Stop will halt the reader processing more events
func (ks *KinesisSource) Stop() {
	ks.log.Warn("Cancelling Kinesis receive ...")
	ks.client.Stop()
}

// GetID returns the identifier for this source
func (ks *KinesisSource) GetID() string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", ks.region, ks.accountID, ks.streamName)
}
