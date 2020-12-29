// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/twitchscience/kinsumer"
	"sync"
	"time"
)

// KinesisSource holds a new client for reading messages from kinesis
type KinesisSource struct {
	Client     *kinsumer.Kinsumer
	StreamName string
	log        *log.Entry
}

// --- Kinsumer overrides

// KinsumerLogrus adds a Logrus logger for Kinsumer
type KinsumerLogrus struct{}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *KinsumerLogrus) Log(format string, v ...interface{}) {
	log.WithFields(log.Fields{"name": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// NewKinesisSource creates a new client for reading messages from kinesis
func NewKinesisSource(region string, streamName string, roleARN string, appName string) (*KinesisSource, error) {
	// TODO: Add statistics monitoring to be able to report on consumer latency
	config := kinsumer.NewConfig().
		WithShardCheckFrequency(10 * time.Second).
		WithLeaderActionFrequency(10 * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{})

	// TODO: See if the client name can be reused to survive same node reboots
	name := uuid.NewV4().String()

	awsSession, awsConfig := getAWSSession(region, roleARN)
	kinesisClient := kinesis.New(awsSession, awsConfig)
	dynamodbClient := dynamodb.New(awsSession, awsConfig)

	k, err := kinsumer.NewWithInterfaces(kinesisClient, dynamodbClient, streamName, appName, name, config)
	if err != nil {
		return nil, err
	}

	return &KinesisSource{
		Client:     k,
		StreamName: streamName,
		log:        log.WithFields(log.Fields{"name": "KinesisSource"}),
	}, nil
}

// Read will pull messages from the noted Kinesis stream forever
func (ks *KinesisSource) Read(sf *SourceFunctions) error {
	ks.log.Infof("Reading messages from stream '%s' ...", ks.StreamName)

	err := ks.Client.Run()
	if err != nil {
		return err
	}

	// TODO: Make the goroutine count configurable
	throttle := make(chan struct{}, 20)
	wg := sync.WaitGroup{}

	for {
		record, checkpointer, err := ks.Client.NextRecordWithCheckpointer()
		if err != nil {
			return fmt.Errorf("k.NextRecordWithCheckpointer returned error: %s", err.Error())
		}

		timePulled := time.Now().UTC()

		ackFunc := func() {
			ks.log.Debugf("Ack'ing record with SequenceNumber: %s", *record.SequenceNumber)
			checkpointer()
		}

		if record != nil {
			timeCreated := record.ApproximateArrivalTimestamp.UTC()
			messages := []*Message{
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
				if err != nil {
					ks.log.Error(err)
				}
				<-throttle
			}()
		} else {
			return nil
		}
	}
	wg.Wait()

	return nil
}

// Stop will halt the reader processing more events
func (ks *KinesisSource) Stop() {
	ks.log.Warn("Cancelling Kinesis receive ...")
	ks.Client.Stop()
}