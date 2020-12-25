// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	log "github.com/sirupsen/logrus"
	"fmt"
	"strings"
)

// SQSTarget holds a new client for writing events to sqs
type SQSTarget struct {
	Client    sqsiface.SQSAPI
	QueueName string
}

// NewSQSTarget creates a new client for writing events to sqs
func NewSQSTarget(region string, queueName string, roleARN string) (*SQSTarget, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	sqsClient := sqs.New(awsSession, awsConfig)

	return &SQSTarget{
		Client:    sqsClient,
		QueueName: queueName,
	}, nil
}

// Write pushes all events to the required target
// TODO: Add event batching (max: 10)
func (st *SQSTarget) Write(events []*Event) (*WriteResult, error) {
	log.Debugf("Writing %d messages to target SQS queue '%s' ...", len(events), st.QueueName)

	urlResult, err := st.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(st.QueueName),
	})
	if err != nil {
		return nil, err
	}
	queueURL := urlResult.QueueUrl

	successes := 0
	failures := 0
	var errstrings []string

	// TODO: Send as batch + asynchronously
	for _, event := range events {
		_, err := st.Client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(string(event.Data)),
			QueueUrl:     queueURL,
		})

		if err != nil {
			errstrings = append(errstrings, err.Error())
			failures++
		} else {
			successes++

			if event.AckFunc != nil {
				event.AckFunc()
			}
		}
	}

	err = nil
	if len(errstrings) > 0 {
		err = fmt.Errorf(strings.Join(errstrings, "\n"))
	}

	log.Debugf("Successfully wrote %d/%d messages to SQS queue '%s'", successes, len(events), st.QueueName)

	return &WriteResult{
		Sent: int64(successes),
		Failed: int64(failures),
	}, err
}

// Close does not do anything for this target
func (st *SQSTarget) Close() {}
