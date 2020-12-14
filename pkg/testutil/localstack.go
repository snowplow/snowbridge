// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package testutil

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

var (
	// AWSLocalstackEndpoint is the default endpoint localstack runs under
	AWSLocalstackEndpoint = "http://localhost:4566"

	// AWSLocalstackRegion is the default region we are using for testing
	AWSLocalstackRegion = "us-east-1"
)

// GetAWSLocalstackSession will return an AWS session ready to interact with localstack
func GetAWSLocalstackSession() *session.Session {
	return session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("foo", "var", ""),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(AWSLocalstackRegion),
		Endpoint:         aws.String(AWSLocalstackEndpoint),
	}))
}

// --- DynamoDB Testing

// GetAWSLocalstackDynamoDBClient returns a DynamoDB client
func GetAWSLocalstackDynamoDBClient() dynamodbiface.DynamoDBAPI {
	return dynamodb.New(GetAWSLocalstackSession())
}

// --- Kinesis Testing

// GetAWSLocalstackKinesisClient returns a Kinesis client
func GetAWSLocalstackKinesisClient() kinesisiface.KinesisAPI {
	return kinesis.New(GetAWSLocalstackSession())
}

// CreateAWSLocalstackKinesisStream creates a new Kinesis stream and polls until
// the stream is in an ACTIVE state
func CreateAWSLocalstackKinesisStream(client kinesisiface.KinesisAPI, streamName string) error {
	_, err := client.CreateStream(&kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int64(1),
	})
	if err != nil {
		return err
	}

	for {
		res, err1 := client.DescribeStream(&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		})
		if err1 != nil {
			return err1
		}

		if *res.StreamDescription.StreamStatus == "ACTIVE" {
			return nil
		}
	}
}

// DeleteAWSLocalstackKinesisStream deletes an existing Kinesis stream
func DeleteAWSLocalstackKinesisStream(client kinesisiface.KinesisAPI, streamName string) (*kinesis.DeleteStreamOutput, error) {
	return client.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
}

// --- SQS Testing

// GetAWSLocalstackSQSClient returns an SQS client
func GetAWSLocalstackSQSClient() sqsiface.SQSAPI {
	return sqs.New(GetAWSLocalstackSession())
}

// SetupAWSLocalstackSQSQueueWithMessages creates a new SQS queue and stubs it with a random set of messages
func SetupAWSLocalstackSQSQueueWithMessages(client sqsiface.SQSAPI, queueName string, messageCount int, messageBody string) *string {
	res, err := CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	for i := 0; i < messageCount; i++ {
		client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(messageBody),
			QueueUrl:     res.QueueUrl,
		})
	}

	return res.QueueUrl
}

// CreateAWSLocalstackSQSQueue creates a new SQS queue
func CreateAWSLocalstackSQSQueue(client sqsiface.SQSAPI, queueName string) (*sqs.CreateQueueOutput, error) {
	return client.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
}

// DeleteAWSLocalstackSQSQueue deletes an existing SQS queue
func DeleteAWSLocalstackSQSQueue(client sqsiface.SQSAPI, queueURL *string) (*sqs.DeleteQueueOutput, error) {
	return client.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: queueURL,
	})
}
