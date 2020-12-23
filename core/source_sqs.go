// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"sync"
)

// SQSSource holds a new client for reading events from SQS
type SQSSource struct {
	Client    sqsiface.SQSAPI
	QueueName string
}

// NewSQSSource creates a new client for reading events from SQS
func NewSQSSource(region string, queueName string, roleARN string) (*SQSSource, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	var sqsClient sqsiface.SQSAPI
	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		sqsClient = sqs.New(sess, &aws.Config{
			Credentials: creds,
			Region:      aws.String(region),
		})
	} else {
		sqsClient = sqs.New(sess)
	}

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

	// TODO: Make the goroutine count configurable
	throttle := make(chan struct{}, 20)
	wg := sync.WaitGroup{}

	// TODO: Need to make gets asynchronous to speed up processing / run multiple getters in parallel
	for {
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
			return err
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

			throttle <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sf.WriteToTarget(events)
				if err != nil {
					log.Error(err)
				}
				<-throttle
			}()
		}
	}
	wg.Wait()
	sf.CloseTarget()

	return nil
}
