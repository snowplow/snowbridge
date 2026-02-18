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

package sourceconfig

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"

	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/stretchr/testify/assert"
)

// TestGetSource_InvalidSource tests that we throw an error when given an invalid source configuration
func TestGetSource_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config with an invalid source name
	hclConfig := []byte(`
		source {
			use "fake_invalid_source" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	source, _, err := GetSource(c, nil)

	assert.Error(err)
	assert.Nil(source)
	assert.Contains(err.Error(), "unknown source: fake_invalid_source")
}

func TestGetSource_WithStdinSource(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		source {
			use "stdin" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	stdinSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(stdinSource)
}

func TestGetSource_WithKafkaSource(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		source {
			use "kafka" {
				brokers         = "my-kafka-connection-string"
				topic_name      = "snowplow-enriched-good"
				consumer_name   = "snowplow-stream-replicator"
				offsets_initial = -2
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	kafkaSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(kafkaSource)
}

func TestGetSource_WithPubsubSource(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8563, nil, t)
	defer func() {
		if err := srv.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()
	defer func() {
		if err := conn.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Define HCL config inline as a string
	hclConfig := []byte(`
		source {
			use "pubsub" {
   				project_id      = "project-test"
  				subscription_id = "test-sub"
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	pubsubSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(pubsubSource)
}

func TestGetSource_WithSQSSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	// Set up localstack SQS queue
	sqsClient := testutil.GetAWSLocalstackSQSClient()
	queueName := "sqs-source-config-test"
	res, createErr := testutil.CreateAWSLocalstackSQSQueue(sqsClient, queueName)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(sqsClient, res.QueueUrl); err != nil {
			t.Logf("Failed to delete queue: %v", err)
		}
	}()

	hclConfig := []byte(fmt.Sprintf(`
		source {
			use "sqs" {
				queue_name          = "%s"
				region              = "%s"
				custom_aws_endpoint = "%s"
			}
		}
	`, queueName, testutil.AWSLocalstackRegion, testutil.AWSLocalstackEndpoint))

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	sqsSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(sqsSource)
}

func TestGetSource_WithHTTPSource(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		source {
			use "http" {
				url  = "localhost:8080"
				path = "/webhook"
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	httpSource, _, err := GetSource(c, nil)

	assert.NoError(err)
	assert.NotNil(httpSource)
}
