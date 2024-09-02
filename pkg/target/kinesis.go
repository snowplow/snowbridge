/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package target

import (
	"fmt"
	"time"

	rand "math/rand/v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
)

const (
	// API Documentation: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html

	// Limited to 500 messages in a single request
	kinesisPutRecordsMaxChunkSize = 500
	// Each record can only be up to 1 MiB in size
	kinesisPutRecordsMessageByteLimit = 1048576
	// Each request can be a maximum of 5 MiB in size total
	kinesisPutRecordsRequestByteLimit = kinesisPutRecordsMessageByteLimit * 5
)

// KinesisTargetConfig configures the destination for records consumed
type KinesisTargetConfig struct {
	StreamName         string `hcl:"stream_name" env:"TARGET_KINESIS_STREAM_NAME"`
	Region             string `hcl:"region" env:"TARGET_KINESIS_REGION"`
	RoleARN            string `hcl:"role_arn,optional" env:"TARGET_KINESIS_ROLE_ARN"`
	RequestMaxMessages int    `hcl:"request_max_messages,optional" env:"TARGET_KINESIS_REQUEST_MAX_MESSAGES"`
	CustomAWSEndpoint  string `hcl:"custom_aws_endpoint,optional" env:"SOURCE_CUSTOM_AWS_ENDPOINT"`
}

// KinesisTarget holds a new client for writing messages to kinesis
type KinesisTarget struct {
	client             kinesisiface.KinesisAPI
	streamName         string
	region             string
	accountID          string
	requestMaxMessages int

	log *log.Entry
}

// newKinesisTarget creates a new client for writing messages to kinesis
func newKinesisTarget(region string, streamName string, roleARN string, customAWSEndpoint string, requestMaxMessages int) (*KinesisTarget, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(region, roleARN, customAWSEndpoint)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.New(awsSession, awsConfig)

	// Restrict chunk sizes to the maximum for a PutRecords request, if configured higher.
	if requestMaxMessages > kinesisPutRecordsMaxChunkSize {
		return nil, errors.New("request_max_messages cannot be higher than the Kinesis PutRecords limit of 500")
	}

	return newKinesisTargetWithInterfaces(kinesisClient, *awsAccountID, region, streamName, requestMaxMessages)
}

// newKinesisTargetWithInterfaces allows you to provide a Kinesis client directly to allow
// for mocking and localstack usage
func newKinesisTargetWithInterfaces(client kinesisiface.KinesisAPI, awsAccountID string, region string, streamName string, requestMaxMessages int) (*KinesisTarget, error) {
	return &KinesisTarget{
		client:             client,
		streamName:         streamName,
		region:             region,
		accountID:          awsAccountID,
		requestMaxMessages: requestMaxMessages,
		log:                log.WithFields(log.Fields{"target": "kinesis", "cloud": "AWS", "region": region, "stream": streamName}),
	}, nil
}

// KinesisTargetConfigFunction creates KinesisTarget from KinesisTargetConfig.
func KinesisTargetConfigFunction(c *KinesisTargetConfig) (*KinesisTarget, error) {
	return newKinesisTarget(c.Region, c.StreamName, c.RoleARN, c.CustomAWSEndpoint, c.RequestMaxMessages)
}

// The KinesisTargetAdapter type is an adapter for functions to be used as
// pluggable components for Kinesis Target. Implements the Pluggable interface.
type KinesisTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f KinesisTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f KinesisTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults if any
	cfg := &KinesisTargetConfig{
		RequestMaxMessages: kinesisPutRecordsMaxChunkSize,
	}

	return cfg, nil
}

// AdaptKinesisTargetFunc returns a KinesisTargetAdapter.
func AdaptKinesisTargetFunc(f func(c *KinesisTargetConfig) (*KinesisTarget, error)) KinesisTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*KinesisTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisTargetConfig")
		}

		return f(cfg)
	}
}

// Write pushes all messages to the required target
func (kt *KinesisTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to stream ...", len(messages))

	chunks, oversized := models.GetChunkedMessages(
		messages,
		kt.requestMaxMessages,
		kt.MaximumAllowedMessageSizeBytes(),
		kinesisPutRecordsRequestByteLimit,
	)

	writeResult := &models.TargetWriteResult{
		Oversized: oversized,
	}

	var errResult error

	for _, chunk := range chunks {
		res, err := kt.process(chunk)
		writeResult = writeResult.Append(res)

		if err != nil {
			errResult = multierror.Append(errResult, err)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to Kinesis stream")
	}

	kt.log.Debugf("Successfully wrote %d/%d messages", writeResult.SentCount, writeResult.Total())
	return writeResult, errResult
}

func (kt *KinesisTarget) process(messages []*models.Message) (*models.TargetWriteResult, error) {
	messageCount := int64(len(messages))
	kt.log.Debugf("Writing chunk of %d messages to stream ...", messageCount)

	messagesToTry := messages
	success := make([]*models.Message, 0)
	nonThrottleFailures := make([]*models.Message, 0)
	errorsEncountered := make([]error, 0)

	retryDelay := 100 * time.Millisecond

	for {
		// We loop through until we have no throttle errors
		entries := make([]*kinesis.PutRecordsRequestEntry, len(messagesToTry))

		for i := 0; i < len(entries); i++ {
			msg := messagesToTry[i]
			entries[i] = &kinesis.PutRecordsRequestEntry{
				Data:         msg.Data,
				PartitionKey: aws.String(msg.PartitionKey),
			}
		}

		requestStarted := time.Now().UTC()
		res, err := kt.client.PutRecords(&kinesis.PutRecordsInput{
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
			// Where the attempt to make a Put request throws an error, treat the whole thing as failed.
			nonThrottleFailures = messagesToTry

			errorsEncountered = append(errorsEncountered, errors.Wrap(err, "Failed to send message batch to Kinesis stream"))
		}

		throttled := make([]*models.Message, 0)
		throttleMsgs := make([]string, 0)
		for i, resultRecord := range res.Records {
			// If we have an error code, check if it's a throttle error
			if resultRecord.ErrorCode != nil {
				switch *resultRecord.ErrorCode {
				case "ProvisionedThroughputExceededException":
					// If we got throttled, add the corresponding record to the list for next retry
					throttled = append(throttled, messagesToTry[i])
					throttleMsgs = append(throttleMsgs, *resultRecord.ErrorMessage)
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

			// Log a warning message about it
			for _, msg := range throttleMsgs {
				throttleWarn = errors.Wrap(throttleWarn, msg)
			}
			kt.log.Warn(throttleWarn.Error())

			// Wait for the delay plus jitter before the next loop
			jitter := time.Duration(1+rand.IntN(30000-1)) * time.Microsecond // any value between 1 microsecond and 30 milliseconds
			time.Sleep(retryDelay + jitter)

			// Extend delay for next loop, to a maximum of 1s
			if retryDelay < 1*time.Second {
				retryDelay = retryDelay + 100*time.Millisecond
			}
		} else {
			// Break the loop and handle results if we have no throttles to retry
			break
		}
	}

	// If we got non-throttle errors, aggregate them so we can surface to the main app flow
	var aggregateErr error

	if len(errorsEncountered) > 0 {
		aggregateErr = errors.New("")
		for _, errToAdd := range errorsEncountered {
			aggregateErr = errors.Wrap(aggregateErr, errToAdd.Error())
		}
	}

	kt.log.Debugf("Successfully wrote %d messages, with %d failures", len(success), len(nonThrottleFailures))
	return models.NewTargetWriteResult(
		success,
		nonThrottleFailures,
		nil,
		nil,
	), aggregateErr
}

// Open does not do anything for this target
func (kt *KinesisTarget) Open() {}

// Close does not do anything for this target
func (kt *KinesisTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (kt *KinesisTarget) MaximumAllowedMessageSizeBytes() int {
	return kinesisPutRecordsMessageByteLimit
}

// GetID returns the identifier for this target
func (kt *KinesisTarget) GetID() string {
	return fmt.Sprintf("arn:aws:kinesis:%s:%s:stream/%s", kt.region, kt.accountID, kt.streamName)
}
