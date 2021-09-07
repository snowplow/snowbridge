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
	// TODO: Maybe wrap the three errors, and return that?
	return nil
}

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

	// TODO: figure out what to do about return values here?
	return nil
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
