// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

const (
	// API Documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html

	// Each message can only be up to 256 KB in size
	sqsSendMessageByteLimit = 262144
)

// SQSTarget holds a new client for writing messages to sqs
type SQSTarget struct {
	client    sqsiface.SQSAPI
	queueURL  *string
	queueName string
	region    string
	accountID string

	log *log.Entry
}

// NewSQSTarget creates a new client for writing messages to sqs
func NewSQSTarget(region string, queueName string, roleARN string) (*SQSTarget, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(region, roleARN)
	if err != nil {
		return nil, err
	}
	sqsClient := sqs.New(awsSession, awsConfig)

	return NewSQSTargetWithInterfaces(sqsClient, *awsAccountID, region, queueName)
}

// NewSQSTargetWithInterfaces allows you to provide an SQS client directly to allow
// for mocking and localstack usage
func NewSQSTargetWithInterfaces(client sqsiface.SQSAPI, awsAccountID string, region string, queueName string) (*SQSTarget, error) {
	return &SQSTarget{
		client:    client,
		queueName: queueName,
		region:    region,
		accountID: awsAccountID,
		log:       log.WithFields(log.Fields{"target": "sqs", "cloud": "AWS", "region": region, "queue": queueName}),
	}, nil
}

// Write pushes all messages to the required target
// TODO: Should each put be in its own goroutine?
func (st *SQSTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to target queue ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		st.MaximumAllowedMessageSizeBytes(),
	)

	var sent []*models.Message
	var failed []*models.Message
	var invalid []*models.Message
	var errResult error

	for _, msg := range safeMessages {
		_, err := st.client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(msg.Data)),
			QueueUrl:     st.queueURL,
		})

		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == sqs.ErrCodeInvalidMessageContents {
					st.log.Warnf("%s: %s", awsErr.Code(), awsErr.Message())

					// Append error to message
					msg.SetError(err)
					invalid = append(invalid, msg)
					continue
				}
			}

			errResult = multierror.Append(errResult, err)

			failed = append(failed, msg)
		} else {
			if msg.AckFunc != nil {
				msg.AckFunc()
			}

			sent = append(sent, msg)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to SQS queue")
	}

	st.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		invalid,
	), errResult
}

// Open fetches the queue URL for this target
func (st *SQSTarget) Open() {
	urlResult, err := st.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(st.queueName),
	})
	if err != nil {
		errWrapped := errors.Wrap(err, "Failed to get SQS queue URL")
		st.log.WithFields(log.Fields{"error": errWrapped}).Fatal(errWrapped)
	}

	st.queueURL = urlResult.QueueUrl
}

// Close resets the queue URL value
func (st *SQSTarget) Close() {
	st.queueURL = nil
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (st *SQSTarget) MaximumAllowedMessageSizeBytes() int {
	return sqsSendMessageByteLimit
}

// GetID returns the identifier for this target
func (st *SQSTarget) GetID() string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", st.region, st.accountID, st.queueName)
}
