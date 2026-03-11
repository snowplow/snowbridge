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

package kinesis

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const (
	// API Documentation: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html

	// Limited to 500 messages in a single request
	kinesisPutRecordsMaxChunkSize = 500
	// Each record can only be up to 1 MiB in size
	kinesisPutRecordsMessageByteLimit = 1048576
	// Each request can be a maximum of 5 MiB in size total
	kinesisPutRecordsRequestByteLimit = kinesisPutRecordsMessageByteLimit * 5

	SupportedTargetKinesis = "kinesis"
)

var (
	provisionedThroughputExceededException = types.ProvisionedThroughputExceededException{}
)

// KinesisTargetConfig configures the destination for records consumed
type KinesisTargetConfig struct {
	BatchingConfig    *targetiface.BatchingConfig `hcl:"batching,block"`
	StreamName        string                      `hcl:"stream_name"`
	Region            string                      `hcl:"region"`
	RoleARN           string                      `hcl:"role_arn,optional"`
	CustomAWSEndpoint string                      `hcl:"custom_aws_endpoint,optional"`
}

// KinesisTargetDriver holds a new client for writing messages to kinesis
type KinesisTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
	client         common.KinesisV2API
	streamName     string
	region         string
	accountID      string

	log *log.Entry
}

// GetDefaultConfiguration returns the default configuration for Kinesis target
func (kt *KinesisTargetDriver) GetDefaultConfiguration() any {
	return &KinesisTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     kinesisPutRecordsMaxChunkSize,
			MaxBatchBytes:        kinesisPutRecordsRequestByteLimit,
			MaxMessageBytes:      kinesisPutRecordsMessageByteLimit,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
	}
}

func (kt *KinesisTargetDriver) SetBatchingConfig(batchingConfig targetiface.BatchingConfig) {
	kt.BatchingConfig = batchingConfig
}

func (kt *KinesisTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return kt.BatchingConfig
}

// InitFromConfig  creates a new client for writing messages to Kinesis
func (kt *KinesisTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*KinesisTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	// Set the batching config - used in both the below and the batcher.
	kt.SetBatchingConfig(*cfg.BatchingConfig)

	awsConfig, awsAccountID, err := common.GetAWSConfig(cfg.Region, cfg.RoleARN, cfg.CustomAWSEndpoint)
	if err != nil {
		return err
	}
	kinesisClient := kinesis.NewFromConfig(*awsConfig)

	// Restrict chunk sizes to the maximum for a PutRecords request, if configured higher.
	if cfg.BatchingConfig.MaxBatchMessages > kinesisPutRecordsMaxChunkSize {
		return errors.New("request_max_messages cannot be higher than the Kinesis PutRecords limit of 500")
	}

	kt.client = kinesisClient
	kt.streamName = cfg.StreamName
	kt.region = cfg.Region
	kt.accountID = awsAccountID
	kt.log = log.WithFields(log.Fields{"target": SupportedTargetKinesis, "cloud": "AWS", "region": cfg.Region, "stream": cfg.StreamName})

	return nil
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (kt *KinesisTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, kt.BatchingConfig)
}

// Write pushes all messages to the required target
func (kt *KinesisTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to stream ...", len(messages))

	messagesToTry := messages
	success := make([]*models.Message, 0)
	nonThrottleFailures := make([]*models.Message, 0)
	errorsEncountered := make([]error, 0)

	retryDelay := 50 * time.Millisecond

	for {
		// We loop through until we have no throttle errors
		entries := make([]types.PutRecordsRequestEntry, len(messagesToTry))

		for i := range entries {
			msg := messagesToTry[i]
			entries[i] = types.PutRecordsRequestEntry{
				Data:         msg.Data,
				PartitionKey: aws.String(msg.PartitionKey),
			}
		}

		requestStarted := time.Now().UTC()
		res, err := kt.client.PutRecords(
			context.Background(),
			&kinesis.PutRecordsInput{
				Records:    entries,
				StreamName: aws.String(kt.streamName),
			})
		requestFinished := time.Now().UTC()

		// Assign timings
		// These will only get recorded in metrics once the messages are successful
		for _, msg := range messagesToTry {
			msg.TimeRequestStarted = requestStarted
			msg.TimeRequestFinished = requestFinished
		}

		if err != nil {
			// When PutRecords request returns an error, treat all messages as failed.
			nonThrottleFailures = messagesToTry
			errorsEncountered = append(errorsEncountered, errors.Wrap(err, "Failed to send message batch to Kinesis stream"))
			break
		}

		throttled := make([]*models.Message, 0)
		throttleMsgs := make([]error, 0)

		for i, resultRecord := range res.Records {
			// If we have an error code, check if it's a throttle error
			if resultRecord.ErrorCode != nil {
				switch *resultRecord.ErrorCode {
				case provisionedThroughputExceededException.ErrorCode():
					// If we got throttled, add the corresponding record to the list for next retry
					throttled = append(throttled, messagesToTry[i])
					throttleMsgs = append(throttleMsgs, errors.New(*resultRecord.ErrorMessage))
				default:
					// If it's a different error, treat it as a failure - retries for this will be handled by the main flow of the app
					errorsEncountered = append(errorsEncountered, errors.New(*resultRecord.ErrorMessage))
					nonThrottleFailures = append(nonThrottleFailures, messagesToTry[i])
				}
			} else {
				// If there is no error, ack and treat as success
				if messagesToTry[i].AckFunc != nil {
					messagesToTry[i].AckFunc()
				}
				success = append(success, messagesToTry[i])
			}
		}
		if len(throttled) > 0 {
			// Assign throttles to be tried next loop
			messagesToTry = throttled

			throttleWarn := errors.New(fmt.Sprintf("hit kinesis throttling, backing off and retrying %v messages", len(throttleMsgs)))
			dedupErr := deduplicateErrMsgWithCounts(throttleMsgs)
			kt.log.Warn(errors.Wrap(throttleWarn, dedupErr.Error()))

			// Wait for the delay plus jitter before the next loop
			jitter := time.Duration(1+rand.IntN(30000-1)) * time.Microsecond // any value between 1 microsecond and 30 milliseconds
			time.Sleep(retryDelay + jitter)

			// Extend delay for next loop, to a maximum of 1s
			if retryDelay < 1*time.Second {
				retryDelay = retryDelay + 50*time.Millisecond
			}
		} else {
			// Break the loop and handle results if we have no throttles to retry
			break
		}
	}

	// If we got non-throttle errors, aggregate them so we can surface to the main app flow
	var aggregateErr error

	if len(errorsEncountered) > 0 {
		aggregateErr = deduplicateErrMsgWithCounts(errorsEncountered)
	}

	kt.log.Debugf("Successfully wrote %d/%d messages, with %d failures", len(success), len(messages), len(nonThrottleFailures))
	return models.NewTargetWriteResult(
		success,
		nonThrottleFailures,
		nil,
	), aggregateErr
}

// Open does not do anything for this target
func (kt *KinesisTargetDriver) Open() error {
	return nil
}

// Close does not do anything for this target
func (kt *KinesisTargetDriver) Close() {}

// deduplicateErrMsgWithCounts returns an error with unique messages together with their occurrence counts appended,
// while preserving first-occurrence order.
func deduplicateErrMsgWithCounts(errs []error) error {
	counts := make(map[string]int, len(errs))
	order := make([]string, 0, len(errs))
	for _, err := range errs {
		errMsg := err.Error()
		if counts[errMsg] == 0 {
			order = append(order, errMsg)
		}
		counts[errMsg]++
	}

	result := make([]string, 0, len(order))
	for _, msg := range order {
		result = append(result, fmt.Sprintf("%s (count: %d)", msg, counts[msg]))
	}

	return errors.New(strings.Join(result, "; "))
}
