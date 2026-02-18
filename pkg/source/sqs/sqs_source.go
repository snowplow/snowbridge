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

package sqssource

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

const SupportedSourceSQS = "sqs"

// Configuration configures the source for records pulled
type Configuration struct {
	QueueName         string `hcl:"queue_name"`
	Region            string `hcl:"region"`
	RoleARN           string `hcl:"role_arn,optional"`
	CustomAWSEndpoint string `hcl:"custom_aws_endpoint,optional"`
}

// sqsSourceDriver holds a new client for reading messages from SQS
type sqsSourceDriver struct {
	sourceiface.SourceChannels
	client   common.SqsV2API
	queueURL string
	log      *log.Entry
}

// DefaultConfiguration returns the default configuration for sqs source
func DefaultConfiguration() Configuration {
	return Configuration{}
}

// BuildFromConfig creates an SQS source from decoded configuration
func BuildFromConfig(cfg *Configuration) (sourceiface.Source, error) {
	awsConfig, _, err := common.GetAWSConfig(cfg.Region, cfg.RoleARN, cfg.CustomAWSEndpoint)
	if err != nil {
		return nil, err
	}

	sqsClient := sqs.NewFromConfig(*awsConfig)

	urlResult, err := sqsClient.GetQueueUrl(
		context.Background(),
		&sqs.GetQueueUrlInput{
			QueueName: aws.String(cfg.QueueName),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get SQS queue URL")
	}

	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	driver := &sqsSourceDriver{
		client:   sqsClient,
		queueURL: *urlResult.QueueUrl,
		log:      log.WithFields(log.Fields{"source": SupportedSourceSQS, "cloud": "AWS", "region": cfg.Region, "queue": cfg.QueueName}),
	}

	return driver, nil
}

// Start will pull messages from the noted SQS queue and process them
func (ss *sqsSourceDriver) Start(ctx context.Context) {
	defer close(ss.MessageChannel)
	ss.log.Info("Reading messages from queue...")

	for {
		select {
		case <-ctx.Done():
			ss.log.Info("Context cancelled, stopping SQS source")
			return
		default:
			msgRes, err := ss.client.ReceiveMessage(
				ctx,
				&sqs.ReceiveMessageInput{
					MessageSystemAttributeNames: []types.MessageSystemAttributeName{
						types.MessageSystemAttributeNameSentTimestamp,
					},
					QueueUrl:            aws.String(ss.queueURL),
					MaxNumberOfMessages: 10,
					VisibilityTimeout:   10,
					WaitTimeSeconds:     1,
				},
			)
			if err != nil {
				// Check if error is due to context cancellation
				if ctx.Err() != nil {
					ss.log.Info("Context cancelled during receive, stopping SQS source")
					return
				}
				ss.log.WithError(err).Error("Failed to get message from SQS queue")
				return
			}

			if len(msgRes.Messages) == 0 {
				continue
			}

			timePulled := time.Now().UTC()

			for _, msg := range msgRes.Messages {
				receiptHandle := msg.ReceiptHandle

				var timeCreated time.Time
				timeCreatedStr, ok := msg.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)]
				if ok {
					timeCreatedMillis, err := strconv.ParseInt(timeCreatedStr, 10, 64)
					if err != nil {
						err = errors.Wrap(err, "Failed to parse SentTimestamp from SQS message")
						ss.log.WithFields(log.Fields{"error": err}).Error(err)

						timeCreated = timePulled
					} else {
						timeCreated = time.Unix(0, timeCreatedMillis*int64(time.Millisecond)).UTC()
					}
				} else {
					ss.log.Warn("Failed to extract SentTimestamp from SQS message attributes")
					timeCreated = timePulled
				}

				message := &models.Message{
					Data:         []byte(*msg.Body),
					PartitionKey: uuid.New().String(),
					AckFunc:      func() { ss.ackMessage(receiptHandle) },
					NackFunc:     func() { ss.nackMessage(receiptHandle) },
					TimeCreated:  timeCreated,
					TimePulled:   timePulled,
				}

				select {
				case <-ctx.Done():
					ss.nackMessage(receiptHandle)
					return
				case ss.MessageChannel <- message:
				}
			}
		}
	}
}

func (ss *sqsSourceDriver) ackMessage(receiptHandle *string) {
	ss.log.Debugf("Deleting message with receipt handle: %s", *receiptHandle)
	_, err := ss.client.DeleteMessage(
		context.Background(),
		&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(ss.queueURL),
			ReceiptHandle: receiptHandle,
		},
	)
	if err != nil {
		err = errors.Wrap(err, "Failed to delete message from SQS queue")
		ss.log.WithFields(log.Fields{"error": err}).Error(err)
	}
}

func (ss *sqsSourceDriver) nackMessage(receiptHandle *string) {
	ss.log.Debugf("Nacking message with receipt handle: %s", *receiptHandle)
	_, err := ss.client.ChangeMessageVisibility(
		context.Background(),

		// Setting visibility timeout to 0, so that this message can be immediately pulled by another consumer.
		&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(ss.queueURL),
			ReceiptHandle:     receiptHandle,
			VisibilityTimeout: 0,
		},
	)
	if err != nil {
		err = errors.Wrap(err, "Failed to nack message from SQS queue")
		ss.log.WithFields(log.Fields{"error": err}).Error(err)
	}
}
