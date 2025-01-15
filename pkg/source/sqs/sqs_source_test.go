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
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

// func newSQSSourceWithInterfaces(client sqsiface.SQSAPI, awsAccountID string, concurrentWrites int, region string, queueName string) (*sqsSource, error) {
func TestNewSQSSourceWithInterfaces_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Since this requires a localstack client (until we implement a mock and make unit tests),
	// We'll only run it with the integration tests for the time being.
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 50, "Hello SQS!!")
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	source, err := newSQSSourceWithInterfaces(client, "00000000000", 10, testutil.AWSLocalstackRegion, queueName)

	assert.IsType(&sqsSource{}, source)
	assert.Nil(err)
}

// newSQSSourceWithInterfaces should fail if we can't reach SQS, commented out this test until we look into https://github.com/snowplow/snowbridge/issues/151
/*
func TestNewSQSSourceWithInterfaces_Failure(t *testing.T) {
	// Unlike the success test, we don't require anything to exist for this one
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	source, err := newSQSSourceWithInterfaces(client, "00000000000", 10, testutil.AWSLocalstackRegion, "nonexistent-queue")

	assert.Nil(source)
	assert.NotNil(err)
}
*/

func TestSQSSource_SetupFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	_, err := newSQSSourceWithInterfaces(client, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists")
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "Failed to get SQS queue URL:")
	}
}

func TestSQSSource_ReadSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 50, "Hello SQS!!")
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	source, err := newSQSSourceWithInterfaces(client, "00000000000", 10, testutil.AWSLocalstackRegion, queueName)
	assert.Nil(err)
	assert.NotNil(source)

	messageCount := 0
	writeFunc := func(messages []*models.Message) error {
		for _, msg := range messages {
			assert.Equal("Hello SQS!!", string(msg.Data))
			messageCount++

			msg.AckFunc()
		}
		return nil
	}
	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	done := make(chan bool)
	go func() {
		err = source.Read(&sf)
		assert.Nil(err)

		done <- true
	}()

	// Wait for the reader to process a batch
	time.Sleep(1 * time.Second)
	source.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("TestSQSSource_ReadSuccess timed out!")
	}

	assert.Equal(50, messageCount)
}

func TestGetSource_WithSQSSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	sqsClient := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-source-config-integration-1"
	_, createErr := testutil.CreateAWSLocalstackSQSQueue(sqsClient, queueName)
	if createErr != nil {
		t.Fatal(createErr)
	}

	defer testutil.DeleteAWSLocalstackSQSQueue(sqsClient, &queueName)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	// Construct the config
	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	configBytesToMerge := []byte(fmt.Sprintf(`
    queue_name = "%s"
    region     = "us-test-1"
`, queueName))

	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(configBytesToMerge, "placeholder")
	if diags.HasErrors() {
		t.Fatalf("failed to parse config bytes")
	}

	c.Data.Source.Use.Name = "sqs"
	c.Data.Source.Use.Body = fileHCL.Body

	sqsSourceConfigFunctionWithLocalStack := configFunctionGeneratorWithInterfaces(sqsClient, "00000000000")
	adaptedHandle := adapterGenerator(sqsSourceConfigFunctionWithLocalStack)

	sqsSourceConfigPairWithInterfaces := config.ConfigurationPair{Name: "sqs", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{sqsSourceConfigPairWithInterfaces}

	source, err := sourceconfig.GetSource(c, supportedSources)
	assert.NotNil(source)
	assert.Nil(err)

	assert.IsType(&sqsSource{}, source)
}

func TestSQSSourceHCL(t *testing.T) {
	testFixPath := filepath.Join(assets.AssetsRootDir, "test", "source", "configs")
	testCases := []struct {
		File     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			File: "source-sqs.hcl",
			Plug: testSQSSourceAdapter(testSQSSourceFunc),
			Expected: &Configuration{
				QueueName:        "testQueue",
				Region:           "us-test-1",
				RoleARN:          "xxx-test-role-arn",
				ConcurrentWrites: 50,
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
func testSQSSourceAdapter(f func(c *Configuration) (*Configuration, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected SQSSourceConfig")
		}

		return f(cfg)
	}

}

func testSQSSourceFunc(c *Configuration) (*Configuration, error) {

	return c, nil
}
