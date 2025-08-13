/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package kinesissource

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twitchscience/kinsumer"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	StreamName              string `hcl:"stream_name"`
	Region                  string `hcl:"region"`
	AppName                 string `hcl:"app_name"`
	RoleARN                 string `hcl:"role_arn,optional"`
	StartTimestamp          string `hcl:"start_timestamp,optional"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (milliseconds optional)
	ReadThrottleDelayMs     int    `hcl:"read_throttle_delay_ms,optional"`
	CustomAWSEndpoint       string `hcl:"custom_aws_endpoint,optional"`
	ShardCheckFreqSeconds   int    `hcl:"shard_check_freq_seconds,optional"`
	LeaderActionFreqSeconds int    `hcl:"leader_action_freq_seconds,optional"`
	ConcurrentWrites        int    `hcl:"concurrent_writes,optional"`
	ClientName              string `hcl:"client_name,optional"`
}

// --- Kinesis source

// kinesisSource holds a new client for reading messages from kinesis
type kinesisSource struct {
	client           *kinsumer.Kinsumer
	streamName       string
	concurrentWrites int
	region           string
	accountID        string

	log *log.Entry
}

// -- Config

// configFunctionGeneratorWithInterfaces generates the kinesis Source Config function, allowing you
// to provide a Kinesis + DynamoDB client directly to allow for mocking and localstack usage
func configFunctionGeneratorWithInterfaces(kinesisClient common.KinesisV2API, dynamodbClient common.DynamoDBV2API, awsAccountID string) func(c *Configuration) (sourceiface.Source, error) {
	// Return a function which returns the source
	return func(c *Configuration) (sourceiface.Source, error) {
		// Handle iteratorTstamp if provided
		var iteratorTstamp time.Time
		var tstampParseErr error
		if c.StartTimestamp != "" {
			iteratorTstamp, tstampParseErr = time.Parse("2006-01-02 15:04:05.999", c.StartTimestamp)
			if tstampParseErr != nil {
				return nil, errors.Wrap(tstampParseErr, fmt.Sprintf("Failed to parse provided value for SOURCE_KINESIS_START_TIMESTAMP: %v", iteratorTstamp))
			}
		}

		return newKinesisSourceWithInterfaces(
			kinesisClient,
			dynamodbClient,
			awsAccountID,
			c.ConcurrentWrites,
			c.Region,
			c.StreamName,
			c.AppName,
			&iteratorTstamp,
			c.ReadThrottleDelayMs,
			c.ShardCheckFreqSeconds,
			c.LeaderActionFreqSeconds,
			c.ClientName)
	}
}

// configFunction returns a kinesis source from a config
func configFunction(c *Configuration) (sourceiface.Source, error) {
	awsConfig, awsAccountID, err := common.GetAWSConfig(c.Region, c.RoleARN, c.CustomAWSEndpoint)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.NewFromConfig(*awsConfig)
	dynamodbClient := dynamodb.NewFromConfig(*awsConfig)

	sourceConfigFunction := configFunctionGeneratorWithInterfaces(
		kinesisClient,
		dynamodbClient,
		awsAccountID)

	return sourceConfigFunction(c)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for Kinesis Source. Implements the Pluggable interface.
type adapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (any, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	// Provide defaults
	cfg := &Configuration{
		ReadThrottleDelayMs:     250, // Kinsumer default is 250ms
		ConcurrentWrites:        50,
		ShardCheckFreqSeconds:   10,
		LeaderActionFreqSeconds: 60,
		ClientName:              uuid.New().String(),
	}

	return cfg, nil
}

// adapterGenerator returns a Kinesis Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for kinesis source")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Kinesis source.
var ConfigPair = config.ConfigurationPair{
	Name:   "kinesis",
	Handle: adapterGenerator(configFunction),
}

// --- Kinsumer overrides

// KinsumerLogrus adds a Logrus logger for Kinsumer
type KinsumerLogrus struct{}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *KinsumerLogrus) Log(format string, v ...any) {
	log.WithFields(log.Fields{"source": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// newKinesisSourceWithInterfaces allows you to provide a Kinesis + DynamoDB client directly to allow
// for mocking and localstack usage
func newKinesisSourceWithInterfaces(
	kinesisClient common.KinesisV2API,
	dynamodbClient common.DynamoDBV2API,
	awsAccountID string,
	concurrentWrites int,
	region string,
	streamName string,
	appName string,
	startTimestamp *time.Time,
	readThrottleDelay int,
	shardCheckFreq int,
	leaderActionFreq int,
	clientName string) (*kinesisSource, error) {

	config := kinsumer.NewConfig().
		WithShardCheckFrequency(time.Duration(shardCheckFreq) * time.Second).
		WithLeaderActionFrequency(time.Duration(leaderActionFreq) * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{}).
		WithIteratorStartTimestamp(startTimestamp).
		WithThrottleDelay(time.Duration(readThrottleDelay) * time.Millisecond)

	k, err := kinsumer.NewWithInterfaces(kinesisClient, dynamodbClient, streamName, appName, clientName, clientName, config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kinsumer client")
	}

	return &kinesisSource{
		client:           k,
		streamName:       streamName,
		concurrentWrites: concurrentWrites,
		region:           region,
		accountID:        awsAccountID,
		log:              log.WithFields(log.Fields{"source": "kinesis", "cloud": "AWS", "region": region, "stream": streamName}),
	}, nil
}

// Read will pull messages from the noted Kinesis stream forever
func (ks *kinesisSource) Read(sf *sourceiface.SourceFunctions) error {
	ks.log.Infof("Reading messages from stream ...")

	err := ks.client.Run()
	if err != nil {
		return errors.Wrap(err, "Failed to start Kinsumer client")
	}

	throttle := make(chan struct{}, ks.concurrentWrites)
	wg := sync.WaitGroup{}

	var kinesisPullErr error
	for {
		record, checkpointer, err := ks.client.NextRecordWithCheckpointer()
		if err != nil {
			kinesisPullErr = errors.Wrap(err, "Failed to pull next Kinesis record from Kinsumer client")
			break
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
					PartitionKey: uuid.New().String(),
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
			break
		}
	}

	// Otherwise, wait for other threads to finish, but force a fatal error if it takes too long.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		break
	case <-time.After(10 * time.Second):
		// Append errors and crash
		if err := multierror.Append(kinesisPullErr, errors.Errorf("wg.Wait() took too long, forcing app close.")); err != nil {
			ks.log.WithFields(log.Fields{"error": err.Error()}).Fatal(err.Error())
		}
	}

	// Return kinesisPullErr if we have one
	if kinesisPullErr != nil {
		return kinesisPullErr
	}

	return nil
}

// Stop will halt the reader processing more events
func (ks *kinesisSource) Stop() {
	ks.log.Warn("Cancelling Kinesis receive ...")
	ks.client.Stop()
}

// GetID returns the identifier for this source
func (ks *kinesisSource) GetID() string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", ks.region, ks.accountID, ks.streamName)
}
