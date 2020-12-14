// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/internal/common"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

// SQSTarget holds a new client for writing messages to sqs
type SQSTarget struct {
	client    sqsiface.SQSAPI
	queueName string

	log *log.Entry
}

// NewSQSTarget creates a new client for writing messages to sqs
func NewSQSTarget(region string, queueName string, roleARN string) (*SQSTarget, error) {
	awsSession, awsConfig := common.GetAWSSession(region, roleARN)
	sqsClient := sqs.New(awsSession, awsConfig)

	return NewSQSTargetWithInterfaces(sqsClient, region, queueName)
}

// NewSQSTargetWithInterfaces allows you to provide an SQS client directly to allow
// for mocking and localstack usage
func NewSQSTargetWithInterfaces(client sqsiface.SQSAPI, region string, queueName string) (*SQSTarget, error) {
	return &SQSTarget{
		client:    client,
		queueName: queueName,
		log:       log.WithFields(log.Fields{"target": "sqs", "cloud": "AWS", "region": region, "queue": queueName}),
	}, nil
}

// Write pushes all messages to the required target
// TODO: Should each put be in its own goroutine?
// TODO: How should oversized records be handled?
func (st *SQSTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to target queue ...", len(messages))

	urlResult, err := st.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(st.queueName),
	})
	if err != nil {
		return models.NewWriteResult(int64(0), int64(len(messages)), messages), errors.Wrap(err, "Failed to get SQS queue URL")
	}
	queueURL := urlResult.QueueUrl

	sent := 0
	failed := 0

	var errResult error

	for _, msg := range messages {
		_, err := st.client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(msg.Data)),
			QueueUrl:     queueURL,
		})

		if err != nil {
			errResult = multierror.Append(errResult, err)
			failed++
		} else {
			sent++
			if msg.AckFunc != nil {
				msg.AckFunc()
			}
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to SQS queue")
	}

	st.log.Debugf("Successfully wrote %d/%d messages", sent, len(messages))
	return models.NewWriteResult(int64(sent), int64(failed), messages), errResult
}

// Open does not do anything for this target
func (st *SQSTarget) Open() {}

// Close does not do anything for this target
func (st *SQSTarget) Close() {}
