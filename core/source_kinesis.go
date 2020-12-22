// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/twitchscience/kinsumer"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// KinesisSource holds a new client for reading events from kinesis
type KinesisSource struct {
	Client     *kinsumer.Kinsumer
	StreamName string
}

// --- Kinsumer overrides

// KinsumerLogrus adds a Logrus logger for Kinsumer
type KinsumerLogrus struct {}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *KinsumerLogrus) Log(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

// NewKinesisSource creates a new client for reading events from kinesis
func NewKinesisSource(region string, streamName string, roleARN string, appName string) (*KinesisSource, error) {
	config := kinsumer.NewConfig().
		WithShardCheckFrequency(10 * time.Second).
		WithLeaderActionFrequency(10 * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{})

	name := uuid.NewV4().String()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	var kinesisClient kinesisiface.KinesisAPI
	var dynamodbClient dynamodbiface.DynamoDBAPI

	if roleARN != "" {
		awsCreds := stscreds.NewCredentials(sess, roleARN)
		awsConfig := aws.Config{
			Credentials: awsCreds,
			Region:      aws.String(region),
		}

		kinesisClient = kinesis.New(sess, &awsConfig)
		dynamodbClient = dynamodb.New(sess, &awsConfig)
	} else {
		kinesisClient = kinesis.New(sess)
		dynamodbClient = dynamodb.New(sess)
	}

	k, err := kinsumer.NewWithInterfaces(kinesisClient, dynamodbClient, streamName, appName, name, config)
	if err != nil {
		return nil, err
	}

	return &KinesisSource{
		Client:     k,
		StreamName: streamName,
	}, nil
}

// Read will pull events from the noted Kinesis stream forever
func (ks *KinesisSource) Read(sf *SourceFunctions) error {
	log.Infof("Reading records from target stream '%s' ...", ks.StreamName)

	err := ks.Client.Run()
	if err != nil {
		return err
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-sig
		log.Warn("SIGTERM called, cancelling Kinesis receive ...")
		ks.Client.Stop()
	}()

	for {
		record, checkpointer, err := ks.Client.NextRecordWithCheckpointer()
		if err != nil {
			return fmt.Errorf("k.NextRecordWithCheckpointer returned error: %s", err.Error())
		}

		ackFunc := func() {
			log.Debugf("Ack'ing record with SequenceNumber: %s", *record.SequenceNumber)
			checkpointer()
		}

		if record != nil {
			events := []*Event{
				{
					Data:         record.Data,
					PartitionKey: *record.PartitionKey,
					AckFunc:      ackFunc,
				},
			}
			err := sf.WriteToTarget(events)
			if err != nil {
				log.Error(err)
			}
		} else {
			return nil
		}
	}

	return nil
}
