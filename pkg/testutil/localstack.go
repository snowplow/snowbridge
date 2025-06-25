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

package testutil

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/common"
)

var (
	// AWSLocalstackEndpoint is the default endpoint localstack runs under
	AWSLocalstackEndpoint = "http://localhost:4566"

	// AWSLocalstackRegion is the default region we are using for testing
	AWSLocalstackRegion = "us-east-1"
)

// GetAWSLocalstackConfig will return an AWS session ready to interact with localstack
// Unlike in SDK v1, S3ForcePathStyle shall be set at service client level
func GetAWSLocalstackConfig() *aws.Config {

	staticCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("foo", "var", ""))
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(staticCreds),
		config.WithRegion(AWSLocalstackRegion),
	)
	if err != nil {
		panic(err)
	}

	cfg.BaseEndpoint = &AWSLocalstackEndpoint
	return &cfg
}

// --- DynamoDB Testing

// DeleteAWSLocalstackDynamoDBTables creates all tables that kinsumer requires.
func DeleteAWSLocalstackDynamoDBTables(client common.DynamoDBV2API, appName string) error {
	_, clientTableError := deleteAWSLocalstackDynamoDBTable(client, appName+"_clients")
	if clientTableError != nil {
		return clientTableError
	}
	_, checkpointTableError := deleteAWSLocalstackDynamoDBTable(client, appName+"_checkpoints")
	if checkpointTableError != nil {
		return checkpointTableError
	}
	_, metadataTableError := deleteAWSLocalstackDynamoDBTable(client, appName+"_metadata")
	if metadataTableError != nil {
		return metadataTableError
	}
	return nil
}

// GetAWSLocalstackDynamoDBClient returns a DynamoDB client
func GetAWSLocalstackDynamoDBClient() common.DynamoDBV2API {
	cfg := GetAWSLocalstackConfig()
	client := dynamodb.NewFromConfig(*cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = &AWSLocalstackEndpoint
	})
	return client
}

// CreateAWSLocalstackDynamoDBTable creates a new Dynamo DB table and polls until
// the table is in an ACTIVE state
func createAWSLocalstackDynamoDBTable(client common.DynamoDBV2API, tableName string, distKey string) error {
	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String(distKey),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String(distKey),
			KeyType:       types.KeyTypeHash,
		}},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String(tableName),
	})
	if err != nil {
		return err
	}

	for {
		res, err1 := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: &tableName})
		if err1 != nil {
			return err1
		}

		if res.Table.TableStatus == "ACTIVE" {
			return nil
		}
	}
}

// DeleteAWSLocalstackDynamoDBTable deletes an existing Dynamo DB table
func deleteAWSLocalstackDynamoDBTable(client common.DynamoDBV2API, tableName string) (*dynamodb.DeleteTableOutput, error) {
	return client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &tableName})
}

// CreateAWSLocalstackDynamoDBTables creates all the DynamoDB tables kinsumer requires and
// polls them until each table is in ACTIVE state
func CreateAWSLocalstackDynamoDBTables(client common.DynamoDBV2API, appName string) error {
	clientTableError := createAWSLocalstackDynamoDBTable(client, appName+"_clients", "ID")
	if clientTableError != nil {
		return clientTableError
	}
	checkpointTableError := createAWSLocalstackDynamoDBTable(client, appName+"_checkpoints", "Shard")
	if checkpointTableError != nil {
		return checkpointTableError
	}
	metadataTableError := createAWSLocalstackDynamoDBTable(client, appName+"_metadata", "Key")
	if metadataTableError != nil {
		return metadataTableError
	}
	return nil
}

// --- Kinesis v2 Testing

// GetAWSLocalstackKinesisClient returns a Kinesis client
func GetAWSLocalstackKinesisClient() common.KinesisV2API {
	cfg := GetAWSLocalstackConfig()
	return kinesis.NewFromConfig(*cfg)
}

// CreateAWSLocalstackKinesisStream creates a new Kinesis stream and polls until
// the stream is in an ACTIVE state
func CreateAWSLocalstackKinesisStream(client common.KinesisV2API, streamName string, shardCount int32) error {
	_, err := client.CreateStream(
		context.Background(),
		&kinesis.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(shardCount),
		},
	)
	if err != nil {
		return err
	}

	for {
		res, err1 := client.DescribeStream(
			context.Background(),
			&kinesis.DescribeStreamInput{
				StreamName: aws.String(streamName),
			},
		)
		if err1 != nil {
			return err1
		}

		if res.StreamDescription.StreamStatus == "ACTIVE" {
			return nil
		}
	}
}

// DeleteAWSLocalstackKinesisStream deletes an existing Kinesis stream
func DeleteAWSLocalstackKinesisStream(client common.KinesisV2API, streamName string) (*kinesis.DeleteStreamOutput, error) {
	return client.DeleteStream(
		context.Background(),
		&kinesis.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
}

// --- SQS v2 Testing

// GetAWSLocalstackSQSClient returns an SQS client
func GetAWSLocalstackSQSClient() common.SqsV2API {
	cfg := GetAWSLocalstackConfig()
	return sqs.NewFromConfig(*cfg)
}

// SetupAWSLocalstackSQSQueueWithMessages creates a new SQS queue and stubs it with a random set of messages
func SetupAWSLocalstackSQSQueueWithMessages(client common.SqsV2API, queueName string, messageCount int, messageBody string) *string {
	res, err := CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	for range messageCount {
		if _, err := client.SendMessage(
			context.Background(),
			&sqs.SendMessageInput{
				DelaySeconds: 0,
				MessageBody:  aws.String(messageBody),
				QueueUrl:     res.QueueUrl,
		}); err != nil {
			logrus.Error(err.Error())
		}
	}

	return res.QueueUrl
}

// PutProvidedDataIntoSQS puts the provided data into an SQS queue
func PutProvidedDataIntoSQS(client common.SqsV2API, queueURL string, data []string) {
	for _, msg := range data {
		if _, err := client.SendMessage(
			context.Background(),
			&sqs.SendMessageInput{
				DelaySeconds: 0,
				MessageBody:  aws.String(msg),
				QueueUrl:     aws.String(queueURL),
		}); err != nil {
			logrus.Error(err.Error())
		}
	}
}

// CreateAWSLocalstackSQSQueue creates a new SQS queue
func CreateAWSLocalstackSQSQueue(client common.SqsV2API, queueName string) (*sqs.CreateQueueOutput, error) {
	return client.CreateQueue(
		context.Background(),
		&sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
		},
	)
}

// DeleteAWSLocalstackSQSQueue deletes an existing SQS queue
func DeleteAWSLocalstackSQSQueue(client common.SqsV2API, queueURL *string) (*sqs.DeleteQueueOutput, error) {
	return client.DeleteQueue(
		context.Background(),
		&sqs.DeleteQueueInput{
			QueueUrl: queueURL,
		},
	)
}

// PutNRecordsIntoKinesis puts n records into a kinesis stream. The records will contain `{dataPrefix} {n}` as their data.
func PutNRecordsIntoKinesis(kinesisClient common.KinesisV2API, n int, streamName string, dataPrefix string) error {
	// Put N records into kinesis stream
	for i := range n {
		_, err := kinesisClient.PutRecord(context.Background(), &kinesis.PutRecordInput{Data: []byte(fmt.Sprint(dataPrefix, " ", i)), PartitionKey: aws.String("abc123"), StreamName: aws.String(streamName)})
		if err != nil {
			return err
		}
	}
	return nil
}

// PutProvidedDataIntoKinesis puts the provided data into a kinsis stream
func PutProvidedDataIntoKinesis(kinesisClient common.KinesisV2API, streamName string, data []string) error {
	// Put N records into kinesis stream
	for _, msg := range data {
		_, err := kinesisClient.PutRecord(context.Background(), &kinesis.PutRecordInput{Data: []byte(msg), PartitionKey: aws.String("abc123"), StreamName: aws.String(streamName)})
		if err != nil {
			return err
		}
	}
	return nil
}
