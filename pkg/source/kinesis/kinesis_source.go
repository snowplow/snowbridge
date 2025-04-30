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
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twitchscience/kinsumer"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	StreamName              string `hcl:"stream_name"`
	Region                  string `hcl:"region"`
	AppName                 string `hcl:"app_name"`
	RoleARN                 string `hcl:"role_arn,optional"`
	StartTimestamp          string `hcl:"start_timestamp,optional"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
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
	statsReceiver    *kinsumerActivityRecorder
	unackedMsgs      map[string]int64
	maxLatency       time.Duration
	mutex            sync.Mutex
	log              *log.Entry
}

// -- Config

// configFunctionGeneratorWithInterfaces generates the kinesis Source Config function, allowing you
// to provide a Kinesis + DynamoDB client directly to allow for mocking and localstack usage
func configFunctionGeneratorWithInterfaces(kinesisClient kinesisiface.KinesisAPI, dynamodbClient dynamodbiface.DynamoDBAPI, awsAccountID string) func(c *Configuration) (sourceiface.Source, error) {
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
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(c.Region, c.RoleARN, c.CustomAWSEndpoint)
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
	return func(i interface{}) (interface{}, error) {
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
func (kl *KinsumerLogrus) Log(format string, v ...interface{}) {
	log.WithFields(log.Fields{"source": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// newKinesisSourceWithInterfaces allows you to provide a Kinesis + DynamoDB client directly to allow
// for mocking and localstack usage
func newKinesisSourceWithInterfaces(
	kinesisClient kinesisiface.KinesisAPI,
	dynamodbClient dynamodbiface.DynamoDBAPI,
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

	statsReceiver := &kinsumerActivityRecorder{}
	config := kinsumer.NewConfig().
		WithShardCheckFrequency(time.Duration(shardCheckFreq) * time.Second).
		WithLeaderActionFrequency(time.Duration(leaderActionFreq) * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&KinsumerLogrus{}).
		WithIteratorStartTimestamp(startTimestamp).
		WithThrottleDelay(time.Duration(readThrottleDelay) * time.Millisecond).
		WithStats(statsReceiver) // to record kinsumer activity and check it's not stuck, see `EventsFromKinesis` function implementation.

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
		statsReceiver:    statsReceiver,
		unackedMsgs:      make(map[string]int64, concurrentWrites),
		maxLatency:       time.Duration(5) * time.Minute, //make configurable
		mutex:            sync.Mutex{},                   //to protect our map of unacked messages in case of concurrent access
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
		randomUUID := uuid.New().String()

		ackFunc := func() {
			ks.log.Debugf("Ack'ing record with SequenceNumber: %s", *record.SequenceNumber)
			checkpointer()
			ks.removeUnacked(randomUUID)
		}

		if record != nil {
			timeCreated := record.ApproximateArrivalTimestamp.UTC()

			messages := []*models.Message{
				{
					Data:         record.Data,
					PartitionKey: randomUUID,
					AckFunc:      ackFunc,
					TimeCreated:  timeCreated,
					TimePulled:   timePulled,
				},
			}

			ks.addUnacked(randomUUID, timePulled)
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
		ks.log.WithFields(log.Fields{"error": kinesisPullErr}).Fatal(kinesisPullErr)
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

type kinsumerActivityRecorder struct {
	lastLiveness *time.Time
}

func (ks *kinsumerActivityRecorder) Checkpoint()                                 {}
func (ks *kinsumerActivityRecorder) EventToClient(inserted, retrieved time.Time) {}

// Called every time after successful fetch executed by kinsumer, even if it a number of records is zero
func (ks *kinsumerActivityRecorder) EventsFromKinesis(num int, shardID string, lag time.Duration) {
	now := time.Now().UTC()
	ks.lastLiveness = &now
}

func (ks *kinesisSource) Health() sourceiface.HealthStatus {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	oldestAllowedTimestamp := time.Now().UTC().Add(-ks.maxLatency).UnixMilli()

	// first check if there is anything pending in memory unacked...
	unackedTimestamps := slices.Collect(maps.Values(ks.unackedMsgs))
	if len(unackedTimestamps) > 0 {
		oldestUnacked := slices.Min(unackedTimestamps)
		if oldestAllowedTimestamp > oldestUnacked {
			return sourceiface.Unhealthy("There is some stuck message being processed now for a while....")
		}
	}

	// if there is nothing left unacked, let's check if kinsumer is healthy and not stuck...
	if ks.statsReceiver.lastLiveness == nil {
		return sourceiface.Unhealthy("We never recorded any activity from kinsumer...")
	}

	// There's been some activity, but it's been quite for a while since now.
	if oldestAllowedTimestamp > ks.statsReceiver.lastLiveness.UnixMilli() {
		return sourceiface.Unhealthy("We haven't recorded any activity from kinsumer for a while...")
	}

	return sourceiface.Healthy()
}

func (ks *kinesisSource) addUnacked(id string, timestamp time.Time) {
	ks.mutex.Lock()
	ks.unackedMsgs[id] = timestamp.UnixMilli()
	ks.mutex.Unlock()
}

func (ks *kinesisSource) removeUnacked(id string) {
	ks.mutex.Lock()
	delete(ks.unackedMsgs, id)
	ks.mutex.Unlock()
}
