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

	kinesisv2 "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	credsv2 "github.com/aws/aws-sdk-go-v2/credentials"
	sqsv2 "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/snowplow/snowbridge/pkg/common"

	"github.com/sirupsen/logrus"
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

// GetAWSLocalstackConfig will return an AWS session ready to interact with localstack
// Unlike in SDK v1, S3ForcePathStyle shall be set at service client level
func GetAWSLocalstackConfig() *awsv2.Config {

	staticCreds := awsv2.NewCredentialsCache(credsv2.NewStaticCredentialsProvider("foo", "var", ""))
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

// GetAWSLocalstackDynamoDBClient returns a DynamoDB client
func GetAWSLocalstackDynamoDBClient() dynamodbiface.DynamoDBAPI {
	return dynamodb.New(GetAWSLocalstackSession())
}

// CreateAWSLocalstackDynamoDBTable creates a new Dynamo DB table and polls until
// the table is in an ACTIVE state
func createAWSLocalstackDynamoDBTable(client dynamodbiface.DynamoDBAPI, tableName string, distKey string) error {
	_, err := client.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{{
			AttributeName: aws.String(distKey),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		}},
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: aws.String(distKey),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		}},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName:   aws.String(tableName),
	})
	if err != nil {
		return err
	}

	for {
		res, err1 := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: &tableName})
		if err1 != nil {
			return err1
		}

		if *res.Table.TableStatus == "ACTIVE" {
			return nil
		}
	}
}

// DeleteAWSLocalstackDynamoDBTable deletes an existing Dynamo DB table
func deleteAWSLocalstackDynamoDBTable(client dynamodbiface.DynamoDBAPI, tableName string) (*dynamodb.DeleteTableOutput, error) {
	return client.DeleteTable(&dynamodb.DeleteTableInput{TableName: &tableName})
}

// CreateAWSLocalstackDynamoDBTables creates all the DynamoDB tables kinsumer requires and
// polls them until each table is in ACTIVE state
func CreateAWSLocalstackDynamoDBTables(client dynamodbiface.DynamoDBAPI, appName string) error {
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

// DeleteAWSLocalstackDynamoDBTables creates all tables that kinsumer requires.
func DeleteAWSLocalstackDynamoDBTables(client dynamodbiface.DynamoDBAPI, appName string) error {
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

// --- Kinesis v1 Testing

// GetAWSLocalstackKinesisClient returns a Kinesis client
func GetAWSLocalstackKinesisClient() kinesisiface.KinesisAPI {
	return kinesis.New(GetAWSLocalstackSession())
}

// CreateAWSLocalstackKinesisStream creates a new Kinesis stream and polls until
// the stream is in an ACTIVE state
func CreateAWSLocalstackKinesisStream(client kinesisiface.KinesisAPI, streamName string, shardCount int64) error {
	_, err := client.CreateStream(&kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int64(shardCount),
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

// --- Kinesis v2 Testing

// GetAWSLocalstackKinesisClient returns a Kinesis client
func GetAWSLocalstackKinesisClientV2() common.KinesisV2API {
	cfg := GetAWSLocalstackConfig()
	return kinesisv2.NewFromConfig(*cfg)
}

// CreateAWSLocalstackKinesisStream creates a new Kinesis stream and polls until
// the stream is in an ACTIVE state
func CreateAWSLocalstackKinesisStreamV2(client common.KinesisV2API, streamName string, shardCount int32) error {
	_, err := client.CreateStream(
		context.Background(),
		&kinesisv2.CreateStreamInput{
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
			&kinesisv2.DescribeStreamInput{
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
func DeleteAWSLocalstackKinesisStreamV2(client common.KinesisV2API, streamName string) (*kinesisv2.DeleteStreamOutput, error) {
	return client.DeleteStream(
		context.Background(),
		&kinesisv2.DeleteStreamInput{
			StreamName: aws.String(streamName),
		})
}

// --- SQS v2 Testing

// GetAWSLocalstackSQSClientV2 returns an SQS client
func GetAWSLocalstackSQSClientV2() common.SqsV2API {
	cfg := GetAWSLocalstackConfig()
	return sqsv2.NewFromConfig(*cfg)
}

// SetupAWSLocalstackSQSQueueWithMessagesV2 creates a new SQS queue and stubs it with a random set of messages
func SetupAWSLocalstackSQSQueueWithMessagesV2(client common.SqsV2API, queueName string, messageCount int, messageBody string) *string {
	res, err := CreateAWSLocalstackSQSQueueV2(client, queueName)
	if err != nil {
		panic(err)
	}

	for range messageCount {
		if _, err := client.SendMessage(
			context.Background(),
			&sqsv2.SendMessageInput{
				DelaySeconds: 0,
				MessageBody:  awsv2.String(messageBody),
				QueueUrl:     res.QueueUrl,
			},
		); err != nil {
			logrus.Error(err.Error())
		}
	}

	return res.QueueUrl
}

// PutProvidedDataIntoSQSV2 puts the provided data into an SQS queue
func PutProvidedDataIntoSQSV2(client common.SqsV2API, queueURL string, data []string) {
	for _, msg := range data {
		if _, err := client.SendMessage(
			context.Background(),
			&sqsv2.SendMessageInput{
				DelaySeconds: 0,
				MessageBody:  awsv2.String(msg),
				QueueUrl:     awsv2.String(queueURL),
			},
		); err != nil {
			logrus.Error(err.Error())
		}
	}
}

// CreateAWSLocalstackSQSQueueV2 creates a new SQS queue
func CreateAWSLocalstackSQSQueueV2(client common.SqsV2API, queueName string) (*sqsv2.CreateQueueOutput, error) {
	return client.CreateQueue(
		context.Background(),
		&sqsv2.CreateQueueInput{
			QueueName: awsv2.String(queueName),
		},
	)
}

// DeleteAWSLocalstackSQSQueueV2 deletes an existing SQS queue
func DeleteAWSLocalstackSQSQueueV2(client common.SqsV2API, queueURL *string) (*sqsv2.DeleteQueueOutput, error) {
	return client.DeleteQueue(
		context.Background(),
		&sqsv2.DeleteQueueInput{
			QueueUrl: queueURL,
		},
	)
}

// --- Kinesis v1 Testing

// PutNRecordsIntoKinesis puts n records into a kinesis stream. The records will contain `{dataPrefix} {n}` as their data.
func PutNRecordsIntoKinesis(kinesisClient kinesisiface.KinesisAPI, n int, streamName string, dataPrefix string) error {
	// Put N records into kinesis stream
	for i := range n {
		_, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{Data: []byte(fmt.Sprint(dataPrefix, " ", i)), PartitionKey: aws.String("abc123"), StreamName: aws.String(streamName)})
		if err != nil {
			return err
		}
	}
	return nil
}

// PutProvidedDataIntoKinesis puts the provided data into a kinsis stream
func PutProvidedDataIntoKinesis(kinesisClient kinesisiface.KinesisAPI, streamName string, data []string) error {
	// Put N records into kinesis stream
	for _, msg := range data {
		_, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{Data: []byte(msg), PartitionKey: aws.String("abc123"), StreamName: aws.String(streamName)})
		if err != nil {
			return err
		}
	}
	return nil
}
