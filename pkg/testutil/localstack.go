/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package testutil

import (
	"fmt"

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

// --- Kinesis Testing

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

// PutProvidedDataIntoSQS puts the provided data into an SQS queue
func PutProvidedDataIntoSQS(client sqsiface.SQSAPI, queueURL string, data []string) {
	for _, msg := range data {
		client.SendMessage(&sqs.SendMessageInput{
			DelaySeconds: aws.Int64(0),
			MessageBody:  aws.String(msg),
			QueueUrl:     aws.String(queueURL),
		})
	}
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

// PutNRecordsIntoKinesis puts n records into a kinesis stream. The records will contain `{dataPrefix} {n}` as their data.
func PutNRecordsIntoKinesis(kinesisClient kinesisiface.KinesisAPI, n int, streamName string, dataPrefix string) error {
	// Put N records into kinesis stream
	for i := 0; i < n; i++ {
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
