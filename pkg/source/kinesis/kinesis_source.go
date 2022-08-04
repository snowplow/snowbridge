// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kinesissource

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/twitchscience/kinsumer"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// configuration configures the source for records pulled
type configuration struct {
	StreamName            string `hcl:"stream_name" env:"SOURCE_KINESIS_STREAM_NAME"`
	Region                string `hcl:"region" env:"SOURCE_KINESIS_REGION"`
	AppName               string `hcl:"app_name" env:"SOURCE_KINESIS_APP_NAME"`
	RoleARN               string `hcl:"role_arn,optional" env:"SOURCE_KINESIS_ROLE_ARN"`
	StartTimestamp        string `hcl:"start_timestamp,optional" env:"SOURCE_KINESIS_START_TIMESTAMP"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
	ConcurrentWrites      int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
	ClientRecordMaxAge    int    `hcl:"client_record_max_age,optional" env:"SOURCE_KINESIS_CLIENT_MAX_AGE"`
	ShardCheckFrequency   int    `hcl:"shard_check_frequency,optional" env:"SOURCE_KINESIS_SHARD_FREQUENCY"`
	LeaderActionFrequency int    `hcl:"leader_action_frequency,optional" env:"SOURCE_KINESIS_LEADER_FREQUENCY"`
}

// --- Kinesis source

// kinesisSource holds a new client for reading messages from kinesis
type kinesisSource struct {
	client                *kinsumer.Kinsumer
	streamName            string
	concurrentWrites      int
	region                string
	accountID             string
	clientRecordMaxAge    int
	shardCheckFrequency   int
	leaderActionFrequency int

	log *log.Entry
}

// -- Config

// configFunctionGeneratorWithInterfaces generates the kinesis Source Config function, allowing you
// to provide a Kinesis + DynamoDB client directly to allow for mocking and localstack usage
func configFunctionGeneratorWithInterfaces(kinesisClient kinesisiface.KinesisAPI, dynamodbClient dynamodbiface.DynamoDBAPI, awsAccountID string) func(c *configuration) (sourceiface.Source, error) {
	// Return a function which returns the source
	return func(c *configuration) (sourceiface.Source, error) {
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
			c.LeaderActionFrequency,
			c.ShardCheckFrequency,
			c.ClientRecordMaxAge)
	}
}

// configFunction returns a kinesis source from a config
func configFunction(c *configuration) (sourceiface.Source, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(c.Region, c.RoleARN)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.New(awsSession, awsConfig)
	dynamodbClient := dynamodb.New(awsSession, awsConfig)

	sourceConfigFunction := configFunctionGeneratorWithInterfaces(
		kinesisClient,
		dynamodbClient,
		*awsAccountID)

	return sourceConfigFunction(c)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for Kinesis Source. Implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &configuration{
		ConcurrentWrites:      50,
		ShardCheckFrequency:   10,
		LeaderActionFrequency: 10,
		ClientRecordMaxAge:    50,
	}

	return cfg, nil
}

// adapterGenerator returns a Kinesis Source adapter.
func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for kinesis source")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Kinesis source.
var ConfigPair = sourceconfig.ConfigPair{
	Name:   "kinesis",
	Handle: adapterGenerator(configFunction),
}

// --- Kinsumer overrides

// KinsumerLogrus adds a Logrus logger for Kinsumer
type KinsumerLogrus struct{}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *KinsumerLogrus) Log(format string, v ...interface{}) {
	log.WithFields(log.Fields{"source": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// newKinesisSourceWithInterfaces allows you to provide a Kinesis + DynamoDB client directly to allow
// for mocking and localstack usage
func newKinesisSourceWithInterfaces(kinesisClient kinesisiface.KinesisAPI, dynamodbClient dynamodbiface.DynamoDBAPI, awsAccountID string, concurrentWrites int, region string, streamName string, appName string, startTimestamp *time.Time, leaderActionFrequency, shardCheckFrequency, clientRecordMaxAge int) (*kinesisSource, error) {
	// TODO: Add statistics monitoring to be able to report on consumer latency

	config := kinsumer.NewConfig().
		WithShardCheckFrequency(time.Duration(shardCheckFrequency) * time.Second).
		WithLeaderActionFrequency(time.Duration(leaderActionFrequency) * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{}).
		WithIteratorStartTimestamp(startTimestamp)

	maxAge := time.Duration(clientRecordMaxAge) * time.Second
	config = config.WithClientRecordMaxAge(&maxAge)

	// TODO: See if the client name can be reused to survive same node reboots
	name := uuid.NewV4().String()

	k, err := kinsumer.NewWithInterfaces(kinesisClient, dynamodbClient, streamName, appName, name, config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kinsumer client")
	}

	return &kinesisSource{
		client:                k,
		streamName:            streamName,
		concurrentWrites:      concurrentWrites,
		region:                region,
		accountID:             awsAccountID,
		log:                   log.WithFields(log.Fields{"source": "kinesis", "cloud": "AWS", "region": region, "stream": streamName}),
		clientRecordMaxAge:    clientRecordMaxAge,
		shardCheckFrequency:   shardCheckFrequency,
		leaderActionFrequency: leaderActionFrequency,
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
		multierror.Append(kinesisPullErr, errors.Errorf("wg.Wait() took too long, forcing app close."))
		ks.log.WithFields(log.Fields{"error": err}).Fatal(err)
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
