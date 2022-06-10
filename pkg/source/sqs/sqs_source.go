// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sqssource

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	"github.com/snowplow-devops/stream-replicator/pkg/common"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// configuration configures the source for records pulled
type configuration struct {
	QueueName        string `hcl:"queue_name" env:"SOURCE_SQS_QUEUE_NAME"`
	Region           string `hcl:"region" env:"SOURCE_SQS_REGION"`
	RoleARN          string `hcl:"role_arn,optional" env:"SOURCE_SQS_ROLE_ARN"`
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}

// sqsSource holds a new client for reading messages from SQS
type sqsSource struct {
	client           sqsiface.SQSAPI
	queueURL         string
	queueName        string
	concurrentWrites int
	region           string
	accountID        string

	log *log.Entry

	// exitSignal holds a channel for signalling an end to the read loop
	exitSignal chan struct{}

	// processErrorSignal holds a channel for handling processing errors
	// and exiting the read loop on the first error discovered
	processErrorSignal chan error
}

// configFunctionGeneratorWithInterfaces generates the SQS Source Config function, allowing you
// to provide an SQS client directly to allow for mocking and localstack usage
func configFunctionGeneratorWithInterfaces(client sqsiface.SQSAPI, awsAccountID string) func(c *configuration) (sourceiface.Source, error) {
	return func(c *configuration) (sourceiface.Source, error) {
		return newSQSSourceWithInterfaces(client, awsAccountID, c.ConcurrentWrites, c.Region, c.QueueName)
	}
}

// configFunction returns an SQS source from a config.
func configFunction(c *configuration) (sourceiface.Source, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(c.Region, c.RoleARN)
	if err != nil {
		return nil, err
	}

	sqsClient := sqs.New(awsSession, awsConfig)

	sourceConfigFunc := configFunctionGeneratorWithInterfaces(sqsClient, *awsAccountID)

	return sourceConfigFunc(c)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for SQS Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &configuration{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// adapterGenerator returns an SQS Source adapter.
func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input, expected SQSSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when and how to build
// an SQS source.
var ConfigPair = sourceconfig.ConfigPair{
	Name:   "sqs",
	Handle: adapterGenerator(configFunction),
}

// newSQSSourceWithInterfaces allows you to provide an SQS client directly to allow
// for mocking and localstack usage
func newSQSSourceWithInterfaces(client sqsiface.SQSAPI, awsAccountID string, concurrentWrites int, region string, queueName string) (*sqsSource, error) {
	return &sqsSource{
		client:             client,
		queueName:          queueName,
		concurrentWrites:   concurrentWrites,
		region:             region,
		accountID:          awsAccountID,
		log:                log.WithFields(log.Fields{"source": "sqs", "cloud": "AWS", "region": region, "queue": queueName}),
		exitSignal:         make(chan struct{}),
		processErrorSignal: make(chan error, concurrentWrites),
	}, nil
}

// Read will pull messages from the noted SQS queue forever
func (ss *sqsSource) Read(sf *sourceiface.SourceFunctions) error {
	ss.log.Info("Reading messages from queue ...")

	urlResult, err := ss.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(ss.queueName),
	})
	if err != nil {
		return errors.Wrap(err, "Failed to get SQS queue URL")
	}
	ss.queueURL = *urlResult.QueueUrl

	throttle := make(chan struct{}, ss.concurrentWrites)
	wg := sync.WaitGroup{}

	var processErr error

ProcessLoop:
	for {
		select {
		case <-ss.exitSignal:
			break ProcessLoop
		case processErr = <-ss.processErrorSignal:
			break ProcessLoop
		default:
			throttle <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ss.process(sf)
				if err != nil {
					ss.processErrorSignal <- err
				}
				<-throttle
			}()
		}
	}
	wg.Wait()

	return processErr
}

func (ss *sqsSource) process(sf *sourceiface.SourceFunctions) error {
	msgRes, err := ss.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(ss.queueURL),
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(1),
	})
	if err != nil {
		return errors.Wrap(err, "Failed to get message from SQS queue")
	}
	timePulled := time.Now().UTC()

	var messages []*models.Message
	for _, msg := range msgRes.Messages {
		receiptHandle := msg.ReceiptHandle

		ackFunc := func() {
			ss.log.Debugf("Deleting message with receipt handle: %s", *receiptHandle)
			_, err := ss.client.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(ss.queueURL),
				ReceiptHandle: receiptHandle,
			})
			if err != nil {
				err = errors.Wrap(err, "Failed to delete message from SQS queue")
				ss.log.WithFields(log.Fields{"error": err}).Error(err)
			}
		}

		var timeCreated time.Time
		timeCreatedStr, ok := msg.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]
		if ok {
			timeCreatedMillis, err := strconv.ParseInt(*timeCreatedStr, 10, 64)
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

		messages = append(messages, &models.Message{
			Data:         []byte(*msg.Body),
			PartitionKey: uuid.NewV4().String(),
			AckFunc:      ackFunc,
			TimeCreated:  timeCreated,
			TimePulled:   timePulled,
		})
	}

	err = sf.WriteToTarget(messages)
	if err != nil {
		ss.log.WithFields(log.Fields{"error": err}).Error(err)
	}
	return nil
}

// Stop will halt the reader processing more events
func (ss *sqsSource) Stop() {
	ss.log.Warn("Cancelling SQS receive ...")
	ss.exitSignal <- struct{}{}
}

// GetID returns the identifier for this source
func (ss *sqsSource) GetID() string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", ss.region, ss.accountID, ss.queueName)
}
