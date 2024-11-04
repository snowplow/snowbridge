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

package kinesissource

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
	config "github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestNewKinesisSourceWithInterfaces_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Since this requires a localstack client (until we implement a mock and make unit tests),
	// We'll only run it with the integration tests for the time being.
	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-1"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "integration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, nil, 250, 10, 10)

	assert.IsType(&kinesisSource{}, source)
	assert.Nil(err)
}

// newKinesisSourceWithInterfaces should fail if we can't reach Kinesis and DDB, commented out this test until we look into https://github.com/snowplow/snowbridge/issues/151
/*
func TestNewKinesisSourceWithInterfaces_Failure(t *testing.T) {
	// Unlike the success test, we don't require anything to exist for this one
	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, "nonexistent-stream", "test", nil)

	assert.Nil(&kinesisSource{}, source)
	assert.NotNil(err)

}
*/

// TODO: When we address https://github.com/snowplow/snowbridge/issues/151, this test will need to change.
func TestKinesisSource_ReadFailure_NoResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists", "fake-name", nil, 250, 10, 10)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/not-exists", source.GetID())

	err = source.Read(nil)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to start Kinsumer client: error describing table fake-name_checkpoints: ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())
	}
}

func TestKinesisSource_ReadMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-integration-2"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "integration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	// Put ten records into kinesis stream
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Test")
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second)

	// Create the source and assert that it's there
	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, nil, 250, 10, 10)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/kinesis-source-integration-2", source.GetID())

	// Read data from stream and check that we got it all
	successfulReads := testutil.ReadAndReturnMessages(source, 3*time.Second, testutil.DefaultTestWriteBuilder, nil)

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

	streamName := "kinesis-source-integration-3"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "integration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	// Put two batches of 10 records into kinesis stream, grabbing a timestamp in between
	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "First batch")
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second) // Put a 1s buffer either side of the start timestamp
	timeToStart := time.Now().UTC()
	time.Sleep(1 * time.Second)

	putErr2 := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, streamName, "Second batch")
	if putErr2 != nil {
		t.Fatal(putErr2)
	}

	// Create the source (with start timestamp) and assert that it's there
	source, err := newKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 15, testutil.AWSLocalstackRegion, streamName, appName, &timeToStart, 250, 10, 10)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/kinesis-source-integration-3", source.GetID())

	// Read from stream
	successfulReads := testutil.ReadAndReturnMessages(source, 3*time.Second, testutil.DefaultTestWriteBuilder, nil)

	// Check that we have ten messages
	assert.Equal(10, len(successfulReads))

	// Check that all messages are from the second batch of Puts
	for _, msg := range successfulReads {
		assert.Contains(string(msg.Data), "Second batch")
	}
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
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "kinesisSourceIntegration"
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)
	if ddbErr != nil {
		t.Fatal(ddbErr)
	}
	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	// Construct the config
	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	configBytesToMerge := []byte(fmt.Sprintf(`
    stream_name = "%s"
    region      = "%s"
    app_name    = "%s"
`, streamName, testutil.AWSLocalstackRegion, appName))

	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(configBytesToMerge, "placeholder")
	if diags.HasErrors() {
		t.Fatalf("failed to parse config bytes")
	}

	c.Data.Source.Use.Name = "kinesis"
	c.Data.Source.Use.Body = fileHCL.Body

	// use our function generator to interact with localstack
	kinesisSourceConfigFunctionWithLocalstack := configFunctionGeneratorWithInterfaces(kinesisClient, dynamodbClient, "00000000000")
	adaptedHandle := adapterGenerator(kinesisSourceConfigFunctionWithLocalstack)

	kinesisSourceConfigPairWithLocalstack := config.ConfigurationPair{Name: "kinesis", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{kinesisSourceConfigPairWithLocalstack}

	source, err := sourceconfig.GetSource(c, supportedSources)
	assert.NotNil(source)
	assert.Nil(err)

	assert.IsType(&kinesisSource{}, source)
}

func TestKinesisSourceHCL(t *testing.T) {
	testFixPath := filepath.Join(assets.AssetsRootDir, "test", "source", "configs")
	testCases := []struct {
		File     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			File: "source-kinesis-simple.hcl",
			Plug: testKinesisSourceAdapter(testKinesisSourceFunc),
			Expected: &Configuration{
				StreamName:              "testStream",
				Region:                  "us-test-1",
				AppName:                 "testApp",
				RoleARN:                 "",
				StartTimestamp:          "",
				ConcurrentWrites:        50,
				ReadThrottleDelayMs:     250,
				ShardCheckFreqSeconds:   10,
				LeaderActionFreqSeconds: 60,
			},
		},
		{
			File: "source-kinesis-extended.hcl",
			Plug: testKinesisSourceAdapter(testKinesisSourceFunc),
			Expected: &Configuration{
				StreamName:              "testStream",
				Region:                  "us-test-1",
				AppName:                 "testApp",
				RoleARN:                 "xxx-test-role-arn",
				StartTimestamp:          "2022-03-15 07:52:53",
				ConcurrentWrites:        51,
				ReadThrottleDelayMs:     250,
				ShardCheckFreqSeconds:   10,
				LeaderActionFreqSeconds: 60,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(testFixPath, tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

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
func testKinesisSourceAdapter(f func(c *Configuration) (*Configuration, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisSourceConfig")
		}

		return f(cfg)
	}

}

func testKinesisSourceFunc(c *Configuration) (*Configuration, error) {

	return c, nil
}
