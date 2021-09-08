// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
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

	// Limited to 10 messages in a single request
	sqsSendMessageBatchChunkSize = 10
	// Each message can only be up to 256 KB in size
	sqsSendMessageByteLimit = 262144
	// Each request can be a maximum of 256 KB in size total
	sqsSendMessageBatchByteLimit = 262144
)

// SQSTarget holds a new client for writing messages to sqs
type SQSTarget struct {
	client    sqsiface.SQSAPI
	queueURL  string
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

	chunks, oversized := models.GetChunkedMessages(
		messages,
		sqsSendMessageBatchChunkSize,
		st.MaximumAllowedMessageSizeBytes(),
		sqsSendMessageBatchByteLimit,
	)

	writeResult := &models.TargetWriteResult{
		Oversized: oversized,
	}

	var errResult error

	for _, chunk := range chunks {
		res, err := st.process(chunk)
		writeResult = writeResult.Append(res)

		if err != nil {
			errResult = multierror.Append(errResult, err)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to SQS queue")
	}

	st.log.Debugf("Successfully wrote %d/%d messages", writeResult.SentCount, writeResult.Total())
	return writeResult, errResult
}

func (st *SQSTarget) process(messages []*models.Message) (*models.TargetWriteResult, error) {
	messageCount := int64(len(messages))
	st.log.Debugf("Writing chunk of %d messages to target queue ...", messageCount)

	lookup := make(map[string]*models.Message)

	entries := make([]*sqs.SendMessageBatchRequestEntry, messageCount)
	for i := 0; i < len(entries); i++ {
		msg := messages[i]
		msgID := strconv.Itoa(i)

		entries[i] = &sqs.SendMessageBatchRequestEntry{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(msg.Data)),
			Id:           aws.String(msgID),
		}
		lookup[msgID] = msg
	}

	res, err := st.client.SendMessageBatch(&sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(st.queueURL),
	})
	if err != nil {
		failed := messages

		return models.NewTargetWriteResult(
			nil,
			failed,
			nil,
			nil,
		), errors.Wrap(err, "Failed to send message batch to SQS queue")
	}

	var sent []*models.Message
	var failed []*models.Message
	var invalid []*models.Message
	var errResult error

	for _, f := range res.Failed {
		msg := lookup[*f.Id]
		fErr := errors.New(fmt.Sprintf("%s: %s", *f.Code, *f.Message))

		if *f.Code == sqs.ErrCodeInvalidMessageContents {
			st.log.Warnf(fErr.Error())

			// Append error to message
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

	st.log.Debugf("Successfully wrote %d/%d messages", len(sent), messageCount)
	return models.NewTargetWriteResult(
		sent,
		failed,
		nil,
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

	st.queueURL = *urlResult.QueueUrl
}

// Close resets the queue URL value
func (st *SQSTarget) Close() {
	st.queueURL = ""
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
