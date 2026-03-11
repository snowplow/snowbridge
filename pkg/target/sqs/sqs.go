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

package sqs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const (
	// API Documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html

	// Limited to 10 messages in a single request
	sqsSendMessageBatchChunkSize = 10
	// Each message can only be up to 1 MB in size
	sqsSendMessageByteLimit = 1048576
	// Each request can be a maximum of 1 MB in size total
	sqsSendMessageBatchByteLimit = 1048576

	SupportedTargetSQS = "sqs"
)

var (
	invalidMsgContents = types.InvalidMessageContents{}
)

// SQSTargetConfig configures the destination for records consumed
type SQSTargetConfig struct {
	BatchingConfig    *targetiface.BatchingConfig `hcl:"batching,block"`
	QueueName         string                      `hcl:"queue_name"`
	Region            string                      `hcl:"region"`
	RoleARN           string                      `hcl:"role_arn,optional"`
	CustomAWSEndpoint string                      `hcl:"custom_aws_endpoint,optional"`
}

// SQSTargetDriver holds a new client for writing messages to sqs
type SQSTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
	client         common.SqsV2API
	queueURL       string
	queueName      string
	region         string
	accountID      string

	log *log.Entry
}

// GetDefaultConfiguration returns the default configuration for SQS target
func (st *SQSTargetDriver) GetDefaultConfiguration() any {
	return &SQSTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     sqsSendMessageBatchChunkSize,
			MaxBatchBytes:        sqsSendMessageBatchByteLimit,
			MaxMessageBytes:      sqsSendMessageByteLimit,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
	}
}

func (st *SQSTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return st.BatchingConfig
}

// InitFromConfig initializes the SQS target driver from configuration
func (st *SQSTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*SQSTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	st.BatchingConfig = *cfg.BatchingConfig

	awsConfig, awsAccountID, err := common.GetAWSConfig(cfg.Region, cfg.RoleARN, cfg.CustomAWSEndpoint)
	if err != nil {
		return err
	}

	sqsClient := sqs.NewFromConfig(*awsConfig)

	st.client = sqsClient
	st.queueName = cfg.QueueName
	st.region = cfg.Region
	st.accountID = awsAccountID
	st.log = log.WithFields(log.Fields{"target": SupportedTargetSQS, "cloud": "AWS", "region": cfg.Region, "queue": cfg.QueueName})

	return nil
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (st *SQSTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, st.BatchingConfig)
}

// Write pushes all messages to the required target
func (st *SQSTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to target queue ...", len(messages))

	lookup := make(map[string]*models.Message)

	entries := make([]types.SendMessageBatchRequestEntry, len(messages))
	for i := 0; i < len(entries); i++ {
		msg := messages[i]
		msgID := strconv.Itoa(i)

		entries[i] = types.SendMessageBatchRequestEntry{
			DelaySeconds: 0,
			MessageBody:  aws.String(string(msg.Data)),
			Id:           aws.String(msgID),
		}
		lookup[msgID] = msg
	}

	requestStarted := time.Now().UTC()
	res, err := st.client.SendMessageBatch(
		context.Background(),
		&sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: aws.String(st.queueURL),
		})
	requestFinished := time.Now().UTC()

	for _, msg := range messages {
		msg.TimeRequestStarted = requestStarted
		msg.TimeRequestFinished = requestFinished
	}

	if err != nil {
		return models.NewTargetWriteResult(
			nil,
			messages,
			nil,
		), errors.Wrap(err, "Error writing messages to SQS queue")
	}

	var sent []*models.Message
	var failed []*models.Message
	var invalid []*models.Message
	var errResult error

	for _, f := range res.Failed {
		msg := lookup[*f.Id]
		fErr := errors.New(fmt.Sprintf("%s: %s", *f.Code, *f.Message))

		if *f.Code == invalidMsgContents.ErrorCode() {
			st.log.Warn(fErr.Error())

			msg.SetError(fErr)
			invalid = append(invalid, msg)
		} else {
			errResult = multierror.Append(errResult, fErr)
			failed = append(failed, msg)
		}

		delete(lookup, *f.Id)
	}

	for _, s := range res.Successful {
		msg := lookup[*s.Id]
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
		sent = append(sent, msg)

		delete(lookup, *s.Id)
	}

	if len(lookup) != 0 {
		st.log.Warnf("Not all messages found in sent batch results; will re-send...")
		for _, msg := range lookup {
			failed = append(failed, msg)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to SQS queue")
	}

	st.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		invalid,
	), errResult
}

// Open fetches the queue URL for this target
func (st *SQSTargetDriver) Open() error {
	urlResult, err := st.client.GetQueueUrl(
		context.Background(),
		&sqs.GetQueueUrlInput{
			QueueName: aws.String(st.queueName),
		},
	)
	if err != nil {
		return errors.Wrap(err, "Failed to get SQS queue URL")
	}

	st.queueURL = *urlResult.QueueUrl
	return nil
}

// Close resets the queue URL value
func (st *SQSTargetDriver) Close() {
	st.queueURL = ""
}
