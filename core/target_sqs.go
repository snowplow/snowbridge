// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	log "github.com/sirupsen/logrus"
	"strings"
)

// SQSTarget holds a new client for writing messages to sqs
type SQSTarget struct {
	Client    sqsiface.SQSAPI
	QueueName string
	log       *log.Entry
}

// NewSQSTarget creates a new client for writing messages to sqs
func NewSQSTarget(region string, queueName string, roleARN string) (*SQSTarget, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	sqsClient := sqs.New(awsSession, awsConfig)

	return &SQSTarget{
		Client:    sqsClient,
		QueueName: queueName,
		log:       log.WithFields(log.Fields{"name": "SQSTarget"}),
	}, nil
}

// Write pushes all messages to the required target
// TODO: Should each put be in its own goroutine?
func (st *SQSTarget) Write(messages []*Message) (*TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to target queue '%s' ...", len(messages), st.QueueName)

	urlResult, err := st.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(st.QueueName),
	})
	if err != nil {
		return nil, err
	}
	queueURL := urlResult.QueueUrl

	sent := 0
	failed := 0
	var errstrings []string

	for _, msg := range messages {
		_, err := st.Client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(msg.Data)),
			QueueUrl:     queueURL,
		})

		if err != nil {
			errstrings = append(errstrings, err.Error())
			failed++
		} else {
			sent++

			if msg.AckFunc != nil {
				msg.AckFunc()
			}
		}
	}

	err = nil
	if len(errstrings) > 0 {
		err = fmt.Errorf(strings.Join(errstrings, "\n"))
	}

	st.log.Debugf("Successfully wrote %d/%d messages to queue '%s'", sent, len(messages), st.QueueName)
	return NewWriteResult(int64(sent), int64(failed), messages), err
}

// Open does not do anything for this target
func (st *SQSTarget) Open() {}

// Close does not do anything for this target
func (st *SQSTarget) Close() {}