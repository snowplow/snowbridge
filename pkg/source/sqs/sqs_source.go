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
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	QueueName         string `hcl:"queue_name"`
	Region            string `hcl:"region"`
	RoleARN           string `hcl:"role_arn,optional"`
	ConcurrentWrites  int    `hcl:"concurrent_writes,optional"`
	CustomAWSEndpoint string `hcl:"custom_aws_endpoint,optional"`
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
func configFunctionGeneratorWithInterfaces(client sqsiface.SQSAPI, awsAccountID string) func(c *Configuration) (sourceiface.Source, error) {
	return func(c *Configuration) (sourceiface.Source, error) {
		return newSQSSourceWithInterfaces(client, awsAccountID, c.ConcurrentWrites, c.Region, c.QueueName)
	}
}

// configFunction returns an SQS source from a config.
func configFunction(c *Configuration) (sourceiface.Source, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(c.Region, c.RoleARN, c.CustomAWSEndpoint)
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
	cfg := &Configuration{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// adapterGenerator returns an SQS Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected SQSSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when and how to build
// an SQS source.
var ConfigPair = config.ConfigurationPair{
	Name:   "sqs",
	Handle: adapterGenerator(configFunction),
}

// newSQSSourceWithInterfaces allows you to provide an SQS client directly to allow
// for mocking and localstack usage
func newSQSSourceWithInterfaces(client sqsiface.SQSAPI, awsAccountID string, concurrentWrites int, region string, queueName string) (*sqsSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
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
			PartitionKey: uuid.New().String(),
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

func (ss *sqsSource) Health() sourceiface.HealthStatus {
  return sourceiface.HealthStatus{ IsHealthy: true}
}

