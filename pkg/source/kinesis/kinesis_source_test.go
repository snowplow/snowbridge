// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kinesissource

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestKinesisSource_ReadFailure_NoResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists", "fake-name", nil)
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
	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, nil)
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
	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, &timeToStart)
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

func TestGetSource_WithKinesisSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-config-integration-1"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName)
	if createErr != nil {
		panic(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "kinesisSourceIntegration"
	testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer os.Unsetenv("SOURCE_NAME")
	defer os.Unsetenv("SOURCE_KINESIS_STREAM_NAME")
	defer os.Unsetenv("SOURCE_KINESIS_REGION")
	defer os.Unsetenv("SOURCE_KINESIS_APP_NAME")

	os.Setenv("SOURCE_NAME", "kinesis")

	os.Setenv("SOURCE_KINESIS_STREAM_NAME", streamName)
	os.Setenv("SOURCE_KINESIS_REGION", testutil.AWSLocalstackRegion)
	os.Setenv("SOURCE_KINESIS_APP_NAME", appName)

	c, err := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	// Use our function generator to interact with localstack
	kinesisSourceConfigFunctionWithLocalstack := configFunctionGeneratorWithInterfaces(kinesisClient, dynamodbClient, "00000000000")
	adaptedHandle := AdaptKinesisSourceFunc(kinesisSourceConfigFunctionWithLocalstack)

	kinesisSourceConfigPairWithLocalstack := sourceconfig.ConfigPair{Name: "kinesis", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{kinesisSourceConfigPairWithLocalstack}

	source, err := sourceconfig.GetSource(c, supportedSources)
	assert.NotNil(source)
	assert.Nil(err)

	assert.IsType(&kinesisSource{}, source)
}

func TestKinesisSourceHCL(t *testing.T) {
	testFixPath := "../../../config/test-fixtures"
	testCases := []struct {
		File     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			File: "source-kinesis-simple.hcl",
			Plug: testKinesisSourceAdapter(testKinesisSourceFunc),
			Expected: &KinesisSourceConfig{
				StreamName:       "testStream",
				Region:           "us-test-1",
				AppName:          "testApp",
				RoleARN:          "",
				StartTimestamp:   "",
				ConcurrentWrites: 50,
			},
		},
		{
			File: "source-kinesis-extended.hcl",
			Plug: testKinesisSourceAdapter(testKinesisSourceFunc),
			Expected: &KinesisSourceConfig{
				StreamName:       "testStream",
				Region:           "us-test-1",
				AppName:          "testApp",
				RoleARN:          "xxx-test-role-arn",
				StartTimestamp:   "2022-03-15 07:52:53",
				ConcurrentWrites: 51,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(testFixPath, tt.File)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			assert.Nil(err)

			use := c.Data.Source.Use
			decoderOpts := &config.DecoderOptions{
				Input: use.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

// Helpers
func testKinesisSourceAdapter(f func(c *KinesisSourceConfig) (*KinesisSourceConfig, error)) KinesisSourceAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*KinesisSourceConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisSourceConfig")
		}

		return f(cfg)
	}

}

func testKinesisSourceFunc(c *KinesisSourceConfig) (*KinesisSourceConfig, error) {

	return c, nil
}
