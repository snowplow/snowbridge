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
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twitchscience/kinsumer"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
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
	ClientName              string `hcl:"client_name,optional"`
	GetRecordsLimit         int    `hcl:"get_records_limit,optional"`
	MaxConcurrentShards     int    `hcl:"max_concurrent_shards,optional"`
}

// DefaultConfiguration returns the default configuration for kinesis source
func DefaultConfiguration() Configuration {
	return Configuration{
		ReadThrottleDelayMs:     250, // Kinsumer default is 250ms
		ShardCheckFreqSeconds:   10,
		LeaderActionFreqSeconds: 60,
		ClientName:              uuid.New().String(),
		GetRecordsLimit:         10000,
		MaxConcurrentShards:     0,
	}
}

// kinesisSourceDriver holds a new client for reading messages from kinesis
type kinesisSourceDriver struct {
	sourceiface.SourceChannels
	client *kinsumer.Kinsumer
	log    *log.Entry
}

// BuildFromConfig creates a kinesis source from decoded configuration
func BuildFromConfig(cfg *Configuration, obs *observer.Observer) (sourceiface.Source, error) {
	awsConfig, _, err := common.GetAWSConfig(cfg.Region, cfg.RoleARN, cfg.CustomAWSEndpoint)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.NewFromConfig(*awsConfig)
	dynamodbClient := dynamodb.NewFromConfig(*awsConfig)

	// Handle iteratorTstamp if provided
	var iteratorTstamp time.Time
	var tstampParseErr error
	if cfg.StartTimestamp != "" {
		iteratorTstamp, tstampParseErr = time.Parse("2006-01-02 15:04:05.999", cfg.StartTimestamp)
		if tstampParseErr != nil {
			return nil, errors.Wrap(tstampParseErr, fmt.Sprintf("Failed to parse provided value for start_timestamp: %v", iteratorTstamp))
		}
	}

	// Build base kinsumer config with available parameters
	config := kinsumer.NewConfig().
		WithShardCheckFrequency(time.Duration(cfg.ShardCheckFreqSeconds) * time.Second).
		WithLeaderActionFrequency(time.Duration(cfg.LeaderActionFreqSeconds) * time.Second).
		WithManualCheckpointing(true).
		WithLogger(&kinsumerLogrus{}).
		WithIteratorStartTimestamp(&iteratorTstamp).
		WithThrottleDelay(time.Duration(cfg.ReadThrottleDelayMs) * time.Millisecond).
		WithGetRecordsLimit(cfg.GetRecordsLimit).
		WithMaxConcurrentShards(cfg.MaxConcurrentShards)

	if obs != nil {
		// If we have an observer, use it for kinsumer metrics
		kinsumerStats := newKinsumerStatsWrapper(obs)
		config = config.WithStats(kinsumerStats)
	}

	// Create kinsumer client
	client, err := kinsumer.NewWithInterfaces(
		kinesisClient,
		dynamodbClient,
		cfg.StreamName,
		cfg.AppName,
		cfg.ClientName,
		cfg.ClientName,
		config,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kinsumer client")
	}

	logger := log.WithFields(log.Fields{"source": "kinesis", "cloud": "AWS", "region": cfg.Region, "stream": cfg.StreamName})

	return &kinesisSourceDriver{
		client: client,
		log:    logger,
	}, nil
}

// Start will pull messages from the noted Kinesis stream forever
func (ks *kinesisSourceDriver) Start(ctx context.Context) {
	defer func() {
		ks.log.Info("Cancelling Kinesis receive ...")

		// Signal for downstream that we're done here
		close(ks.MessageChannel)

		// Stop() on kinsumer under the hood waits for all pulled messages to be checkpointed
		if ks.client != nil {
			ks.client.Stop()
		}
	}()

	ks.log.Infof("Reading messages from stream ...")

	err := ks.client.Run()
	if err != nil {
		ks.log.WithError(err).Error("Failed to start Kinsumer client")
		ks.client = nil
		return
	}

	ks.log.Debug("Kinsumer client initialized successfully")

	// Populate kinsumer messages channel in a background...
	kinsumerMessages := make(chan *models.Message)
	go ks.pullMessagesFromKinsumer(ctx, kinsumerMessages)

	// ... and consume internal kinsumer messages here, publish to the public output channel, respecting provided context cancellation
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-kinsumerMessages:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case ks.MessageChannel <- message:
			}
		}
	}
}

func (ks *kinesisSourceDriver) pullMessagesFromKinsumer(ctx context.Context, output chan<- *models.Message) {
	defer close(output)

	for {
		record, checkpointer, err := ks.client.NextRecordWithCheckpointer()
		if err != nil {
			ks.log.WithError(err).Error("Failed to pull next Kinesis record from Kinsumer client")
			return
		}

		if record == nil {
			return
		}

		ackFunc := func() {
			ks.log.Debugf("Ack'ing record with SequenceNumber: %s", *record.SequenceNumber)

			// From kinsumer's NextRecordWithCheckpointer: https://github.com/snowplow-devops/kinsumer/blob/v1.7.0/kinsumer.go#L690:
			// 'WARNING: checkpointer() can block indefinitely if not called in order.'

			// Call checkpointer asynchronously to avoid blocking the calling thread.
			// Downstream concurrent transformation (e.g. with multiple transformer workers in the pool) may cause messages to be reordered and then acked out of original order by targets,
			// but kinsumer's updateFunc ensures checkpoints are written to DynamoDB sequentially.
			// See: https://github.com/snowplow-devops/kinsumer/blob/v1.7.0/checkpoints.go#L274-L298
			go checkpointer()
		}

		message := &models.Message{
			Data:         record.Data,
			PartitionKey: uuid.New().String(),
			AckFunc:      ackFunc,
			TimeCreated:  record.ApproximateArrivalTimestamp.UTC(),
			TimePulled:   time.Now().UTC(),
		}
		select {
		case <-ctx.Done():
			return
		case output <- message:
		}
	}
}
