// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package source

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestKinesisSource_ReadFailure_NoResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	source, err := NewKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists", "fake-name", nil)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/not-exists", source.GetID())

	err = source.Read(nil)
	assert.NotNil(err)
	assert.Equal("Failed to start Kinsumer client: error describing table fake-name_checkpoints: ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())
}

func TestKinesisSource_ReadMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-1"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName)
	if createErr != nil {
		panic(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "integration"
	testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	// Put ten records into kinesis stream
	putErr := putNRecordsIntoKinesis(kinesisClient, 10, streamName, "Test")
	if putErr != nil {
		panic(putErr)
	}

	// Create the source and assert that it's there
	source, err := NewKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, nil)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/kinesis-source-integration-1", source.GetID())

	// Read data from stream and check that we got it all
	successfulReads := testutil.ReadAndReturnMessages(source)

	assert.Equal(10, len(successfulReads))
}

func TestKinesisSource_StartTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-2"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName)
	if createErr != nil {
		panic(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "integration"
	testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	// Put two batches of 10 records into kinesis stream, grabbing a timestamp in between
	putErr := putNRecordsIntoKinesis(kinesisClient, 10, streamName, "First batch")
	if putErr != nil {
		panic(putErr)
	}

	time.Sleep(1 * time.Second) // Put a 1s buffer either side of the start timestamp
	timeToStart := time.Now()
	time.Sleep(1 * time.Second)

	putErr2 := putNRecordsIntoKinesis(kinesisClient, 10, streamName, "Second batch")
	if putErr2 != nil {
		panic(putErr2)
	}

	// Create the source (with start timestamp) and assert that it's there
	source, err := NewKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, &timeToStart)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/kinesis-source-integration-2", source.GetID())

	// Read from stream
	successfulReads := testutil.ReadAndReturnMessages(source)

	// Check that we have ten messages
	assert.Equal(10, len(successfulReads))

	// Check that all messages are from the second batch of Puts
	for _, msg := range successfulReads {
		assert.Contains(string(msg.Data), "Second batch")
	}
}

func putNRecordsIntoKinesis(kinesisClient kinesisiface.KinesisAPI, n int, streamName string, dataPrefix string) error {
	// Put N records into kinesis stream
	for i := 0; i < n; i++ {
		_, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{Data: []byte(fmt.Sprint(dataPrefix, " ", i)), PartitionKey: aws.String("abc123"), StreamName: aws.String(streamName)})
		if err != nil {
			return err
		}
	}
	return nil
}
