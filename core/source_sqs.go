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
	"github.com/twinj/uuid"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// SQSSource holds a new client for reading events from SQS
type SQSSource struct {
	Client    sqsiface.SQSAPI
	QueueName string
}

// NewSQSSource creates a new client for reading events from SQS
func NewSQSSource(region string, queueName string, roleARN string) (*SQSSource, error) {
	awsSession, awsConfig := getAWSSession(region, roleARN)
	sqsClient := sqs.New(awsSession, awsConfig)

	return &SQSSource{
		Client:    sqsClient,
		QueueName: queueName,
	}, nil
}

// Read will pull events from the noted SQS queue forever
func (ss *SQSSource) Read(sf *SourceFunctions) error {
	log.Infof("Reading records from SQS queue '%s' ...", ss.QueueName)

	urlResult, err := ss.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(ss.QueueName),
	})
	if err != nil {
		return err
	}
	queueURL := urlResult.QueueUrl

	sig := make(chan os.Signal)
	exitSignal := make(chan struct{})
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-sig
		log.Warn("SIGTERM called, cancelling SQS receive ...")
		exitSignal <- struct{}{}
	}()

	// TODO: Make the goroutine count configurable
	throttle := make(chan struct{}, 20)
	wg := sync.WaitGroup{}

ProcessLoop:
	for {
		select {
		case <-exitSignal:
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
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(1),
	})
	if err != nil {
		log.Error(err)
		return
	}

	for _, msg := range msgRes.Messages {
		receiptHandle := msg.ReceiptHandle

		ackFunc := func() {
			log.Debugf("Deleting message with receipt handle: %s", *receiptHandle)
			_, err := ss.Client.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: receiptHandle,
			})
			if err != nil {
				log.Error(err)
			}
		}

		events := []*Event{
			{
				Data:         []byte(*msg.Body),
				PartitionKey: uuid.NewV4().String(),
				AckFunc:      ackFunc,
			},
		}

		err := sf.WriteToTarget(events)
		if err != nil {
			log.Error(err)
		}
	}
}
