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

// NewKinesisSource creates a new client for reading events from kinesis
func NewKinesisSource(region string, streamName string, roleARN string, appName string) (*KinesisSource, error) {
	// TODO: Add custom logger?
	// TODO: Should we override other settings here?
	config := kinsumer.NewConfig().WithShardCheckFrequency(10 * time.Second).WithLeaderActionFrequency(10 * time.Second)

	// TODO: Should this name map to a particular instance id?
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
	log.Infof("Reading messages from target stream '%s' ...", ks.StreamName)

	err := ks.Client.Run()
	if err != nil {
		return err
	}

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-sig
		log.Warn("SIGTERM called, cancelling Kinesis receive ...")
		// TODO: Can we wait for the buffer to flush?
		ks.Client.Stop()
	}()

	for {
		record, err := ks.Client.Next()
		if err != nil {
			return fmt.Errorf("k.Next returned error: %s", err.Error())
		}

		if record != nil {
			// TODO: Can we get the partition key?
			// TODO: What to do on error?
			events := []*Event{
				{
					Data:         record,
					PartitionKey: uuid.NewV4().String(),
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
