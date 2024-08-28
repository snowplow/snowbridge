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
// TODO: Should each put be in its own goroutine?
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

	entries := make([]*kinesis.PutRecordsRequestEntry, messageCount)
	for i := 0; i < len(entries); i++ {
		msg := messages[i]
		entries[i] = &kinesis.PutRecordsRequestEntry{
			Data:         msg.Data,
			PartitionKey: aws.String(msg.PartitionKey),
		}
	}

	requestStarted := time.Now()
	res, err := kt.client.PutRecords(&kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(kt.streamName),
	})
	requestFinished := time.Now()

	for _, msg := range messages {
		msg.TimeRequestStarted = requestStarted
		msg.TimeRequestFinished = requestFinished
	}

	if err != nil {
		failed := messages

		return models.NewTargetWriteResult(
			nil,
			failed,
			nil,
			nil,
		), errors.Wrap(err, "Failed to send message batch to Kinesis stream")
	}

	// Hack to test if retries fix the problem

	backoff := 100 * time.Millisecond
	// infinitely loop
	for {
		// Cap backoff to 1s max
		if backoff > 1*time.Second {
			backoff = 1 * time.Second
		}
		// TODO: Can we ack successful messages when some fail in the batch? This will cause duplicate processing on failure.
		if res.FailedRecordCount != nil && *res.FailedRecordCount > int64(0) {
			// if we have any failure, log it and retry the same request on a backoff (for our hacky test this is fine)

			// Log the error
			kt.log.Warnf("Hit kinesis error: %s", *res.Records[0].ErrorMessage)

			// back off
			time.Sleep(backoff)

			// retry the request
			res2, err := kt.client.PutRecords(&kinesis.PutRecordsInput{
				Records:    entries,
				StreamName: aws.String(kt.streamName),
			})

			// If the request errors, fail
			if err != nil {
				failed := messages

				return models.NewTargetWriteResult(
					nil,
					failed,
					nil,
					nil,
				), errors.Wrap(err, "Failed to send message batch to Kinesis stream")
			}

			// Overwrite old res with new
			res = res2

			// increase the backoff
			backoff = backoff * 2

			// failed := messages

			// // Wrap produces nil if the initial error is nil, so create an empty error instead
			// kinesisErrs := errors.New("")

			// for _, record := range res.Records {
			// 	if record.ErrorMessage != nil {
			// 		kinesisErrs = errors.Wrap(kinesisErrs, *record.ErrorMessage)
			// 	}
			// }

			// return models.NewTargetWriteResult(
			// 	nil,
			// 	failed,
			// 	nil,
			// 	nil,
			// ), errors.Wrap(kinesisErrs, "Failed to write all messages in batch to Kinesis stream")
		} else {
			// if we no longer have a failure, exit the loop
			break
		}
	}

	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	sent := messages

	kt.log.Debugf("Successfully wrote %d messages", len(entries))
	return models.NewTargetWriteResult(
		sent,
		nil,
		nil,
		nil,
	), nil
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
