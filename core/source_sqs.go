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
	"github.com/twinj/uuid"
	"strconv"
	"sync"
	"time"
)

// SQSSource holds a new client for reading messages from SQS
type SQSSource struct {
	Client           sqsiface.SQSAPI
	QueueName        string
	concurrentWrites int
	log              *log.Entry

	// exitSignal holds a channel for signalling an end to the read loop
	exitSignal chan struct{}
}

// NewSQSSource creates a new client for reading messages from SQS
func NewSQSSource(concurrentWrites int, region string, queueName string, roleARN string) (*SQSSource, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	sqsClient := sqs.New(awsSession, awsConfig)

	return &SQSSource{
		Client:           sqsClient,
		QueueName:        queueName,
		concurrentWrites: concurrentWrites,
		log:              log.WithFields(log.Fields{"name": "SQSSource"}),
		exitSignal:       make(chan struct{}),
	}, nil
}

// Read will pull messages from the noted SQS queue forever
func (ss *SQSSource) Read(sf *SourceFunctions) error {
	ss.log.Infof("Reading messages from queue '%s' ...", ss.QueueName)

	urlResult, err := ss.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(ss.QueueName),
	})
	if err != nil {
		return err
	}
	queueURL := urlResult.QueueUrl

	throttle := make(chan struct{}, ss.concurrentWrites)
	wg := sync.WaitGroup{}

ProcessLoop:
	for {
		select {
		case <-ss.exitSignal:
			break ProcessLoop
		default:
			throttle <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				ss.process(queueURL, sf)
				<-throttle
			}()
		}
	}
	wg.Wait()

	return nil
}

func (ss *SQSSource) process(queueURL *string, sf *SourceFunctions) {
	msgRes, err := ss.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(1),
	})
	if err != nil {
		ss.log.Error(err)
		return
	}
	timePulled := time.Now().UTC()

	var messages []*Message
	for _, msg := range msgRes.Messages {
		receiptHandle := msg.ReceiptHandle

		ackFunc := func() {
			ss.log.Debugf("Deleting message with receipt handle: %s", *receiptHandle)
			_, err := ss.Client.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: receiptHandle,
			})
			if err != nil {
				ss.log.Error(err)
			}
		}

		var timeCreated time.Time
		timeCreatedStr, ok := msg.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]
		if ok {
			timeCreatedMillis, err := strconv.ParseInt(*timeCreatedStr, 10, 64)
			if err != nil {
				ss.log.Error(fmt.Sprintf("Error extracting SentTimestamp from message attributes, latency measurements will not be accurate!"))
				timeCreated = timePulled
			} else {
				timeCreated = time.Unix(0, timeCreatedMillis*int64(time.Millisecond)).UTC()
			}
		} else {
			ss.log.Warnf("Could not extract SentTimestamp from message attributes, latency measurements will not be accurate!")
			timeCreated = timePulled
		}

		messages = append(messages, &Message{
			Data:         []byte(*msg.Body),
			PartitionKey: uuid.NewV4().String(),
			AckFunc:      ackFunc,
			TimeCreated:  timeCreated,
			TimePulled:   timePulled,
		})
	}

	err = sf.WriteToTarget(messages)
	if err != nil {
		ss.log.Error(err)
	}
}

// Stop will halt the reader processing more events
func (ss *SQSSource) Stop() {
	ss.log.Warn("Cancelling SQS receive ...")
	ss.exitSignal <- struct{}{}
}
