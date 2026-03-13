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

package targetconfig

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/config"
	httpTarget "github.com/snowplow/snowbridge/v3/pkg/target/http"
	"github.com/snowplow/snowbridge/v3/pkg/target/kafka"
	"github.com/snowplow/snowbridge/v3/pkg/target/stdout"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestGetTarget_Stdout(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "stdout" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(1, batchingConfig.MaxBatchMessages)
	assert.Equal(1048576, batchingConfig.MaxBatchBytes)
	assert.Equal(1048576, batchingConfig.MaxMessageBytes)
	assert.Equal(1, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)

	// Verify the driver configuration (dataOnlyOutput should be false by default)
	stdoutTarget := tar.TargetDriver
	stdoutTargetValue := reflect.ValueOf(stdoutTarget).Elem()
	dataOnlyOutputField := stdoutTargetValue.FieldByName("dataOnlyOutput")
	assert.True(dataOnlyOutputField.IsValid())
	assert.False(dataOnlyOutputField.Bool())
}

func TestGetTarget_Kafka(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	topicName := "snowplow-enriched-good"

	// Create kafka topic
	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			fmt.Println(err.Error())
		}
	}()

	err2 := adminClient.CreateTopic(topicName, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err2 != nil {
		panic(err2)
	}
	defer func() {
		if err := adminClient.DeleteTopic(topicName); err != nil {
			fmt.Println(err.Error())
		}
	}()

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "kafka" {
				brokers = "localhost:9092"
				topic_name = "snowplow-enriched-good"
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(100, batchingConfig.MaxBatchMessages)
	assert.Equal(1048576, batchingConfig.MaxBatchBytes)
	assert.Equal(1048576, batchingConfig.MaxMessageBytes)
	assert.Equal(5, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)
	// Verify the driver configuration (dataOnlyOutput should be false by default)
	kafkaTarget := tar.TargetDriver.(*kafka.KafkaTargetDriver)
	kafkaTargetValue := reflect.ValueOf(kafkaTarget).Elem()

	brokersField := kafkaTargetValue.FieldByName("brokers")
	assert.True(brokersField.IsValid())
	assert.Equal("localhost:9092", brokersField.String())

	topicNameField := kafkaTargetValue.FieldByName("topicName")
	assert.True(topicNameField.IsValid())
	assert.Equal("snowplow-enriched-good", topicNameField.String())
}

func TestGetTarget_Kinesis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	var hclConfig []byte
	hclConfig = fmt.Appendf(hclConfig, `
		target {
			use "kinesis" {
				batching {
					max_batch_bytes = 99999
					max_message_bytes = 9999
					max_batch_messages = 99
					max_concurrent_batches = 10
					flush_period_millis = 10
				}
   				stream_name = "my-stream"
    			region      = "%s"
    			custom_aws_endpoint = "%s"
			}
		}
	`, testutil.AWSLocalstackRegion, testutil.AWSLocalstackEndpoint)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has correct batching configuration
	assert.Equal(99, batchingConfig.MaxBatchMessages)
	assert.Equal(99999, batchingConfig.MaxBatchBytes)
	assert.Equal(9999, batchingConfig.MaxMessageBytes)
	assert.Equal(10, batchingConfig.MaxConcurrentBatches)
	assert.Equal(10, batchingConfig.FlushPeriodMillis)
}

func TestGetTarget_BatchingOverrides(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "stdout" {
				batching {
					max_batch_bytes = 99999
					max_message_bytes = 9999
					max_batch_messages = 999
					max_concurrent_batches = 99
					flush_period_millis = 9
				}
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target has the correct default batching configuration
	assert.Equal(99999, tar.TargetDriver.(*stdout.StdoutTargetDriver).BatchingConfig.MaxBatchBytes)
	assert.Equal(9999, tar.TargetDriver.(*stdout.StdoutTargetDriver).BatchingConfig.MaxMessageBytes)
	assert.Equal(999, tar.TargetDriver.(*stdout.StdoutTargetDriver).BatchingConfig.MaxBatchMessages)
	assert.Equal(99, tar.TargetDriver.(*stdout.StdoutTargetDriver).BatchingConfig.MaxConcurrentBatches)
	assert.Equal(9, tar.TargetDriver.(*stdout.StdoutTargetDriver).BatchingConfig.FlushPeriodMillis)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)
	// Verify the driver configuration (dataOnlyOutput should be false by default)
	stdoutTarget := tar.TargetDriver.(*stdout.StdoutTargetDriver)
	stdoutTargetValue := reflect.ValueOf(stdoutTarget).Elem()
	dataOnlyOutputField := stdoutTargetValue.FieldByName("dataOnlyOutput")
	assert.True(dataOnlyOutputField.IsValid())
	assert.False(dataOnlyOutputField.Bool())
}

func TestGetTarget_InvalidBatching(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "stdout" {
				batching {
					max_batch_bytes = 99999
					max_message_bytes = 100000
					max_batch_messages = 999
					max_concurrent_batches = 99
					flush_period_millis = 9
				}
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.Nil(tar)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "invalid batching configuration")
	}
}

func TestGetTarget_PubSub(t *testing.T) {
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
		target {
			use "pubsub" {
				project_id = "project-test"
				topic_name = "test-topic"
				batching {
					max_batch_messages = 250
					max_batch_bytes = 5242880
					max_message_bytes = 2097152
					max_concurrent_batches = 10
					flush_period_millis = 1000
				}
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid pubsub config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct batching configuration (different from defaults)
	assert.Equal(250, batchingConfig.MaxBatchMessages)
	assert.Equal(5242880, batchingConfig.MaxBatchBytes)
	assert.Equal(2097152, batchingConfig.MaxMessageBytes)
	assert.Equal(10, batchingConfig.MaxConcurrentBatches)
	assert.Equal(1000, batchingConfig.FlushPeriodMillis)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)
}

func TestGetTarget_PubSub_DefaultBatching(t *testing.T) {
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

	// Define HCL config inline as a string (without explicit batching block)
	hclConfig := []byte(`
		target {
			use "pubsub" {
				project_id = "project-test"
				topic_name = "test-topic"
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid pubsub config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(100, batchingConfig.MaxBatchMessages)
	assert.Equal(10485760, batchingConfig.MaxBatchBytes)
	assert.Equal(10485760, batchingConfig.MaxMessageBytes)
	assert.Equal(5, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)
}

func TestGetTarget_HTTP(t *testing.T) {
	assert := assert.New(t)

	// Create a simple test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	// Define HCL config inline as a string
	hclConfig := []byte(fmt.Sprintf(`
		target {
			use "http" {
				url = "%s"
			}
		}
	`, server.URL))

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	if err != nil {
		t.Fatalf("Failed to parse HCL config: %v", err)
	}
	assert.NotNil(c)

	// Call GetTarget with valid HTTP config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	if err != nil {
		t.Fatalf("Failed to get target: %v", err)
	}
	assert.NotNil(tar)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(50, batchingConfig.MaxBatchMessages)
	assert.Equal(1048576, batchingConfig.MaxBatchBytes)
	assert.Equal(1048576, batchingConfig.MaxMessageBytes)
	assert.Equal(5, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)

	// Verify the driver configuration
	httpTargetDriver := tar.TargetDriver.(*httpTarget.HTTPTargetDriver)
	httpTargetValue := reflect.ValueOf(httpTargetDriver).Elem()

	urlField := httpTargetValue.FieldByName("httpURL")
	assert.True(urlField.IsValid())
	assert.Equal(server.URL, urlField.String())
}

func TestGetTarget_SQS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// So that we can access localstack
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	hclConfig := []byte(fmt.Sprintf(`
		target {
			use "sqs" {
				queue_name = "test-queue"
				region = "%s"
				custom_aws_endpoint = "%s"
			}
		}
	`, testutil.AWSLocalstackRegion, testutil.AWSLocalstackEndpoint))

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid SQS config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(10, batchingConfig.MaxBatchMessages)
	assert.Equal(1048576, batchingConfig.MaxBatchBytes)
	assert.Equal(1048576, batchingConfig.MaxMessageBytes)
	assert.Equal(5, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)
}

func TestGetTarget_EventHub(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=fake")

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "eventhub" {
    			namespace = "testNamespace"
   				name      = "testName"
			}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(500, batchingConfig.MaxBatchMessages)
	assert.Equal(1048576, batchingConfig.MaxBatchBytes)
	assert.Equal(1048576, batchingConfig.MaxMessageBytes)
	assert.Equal(5, batchingConfig.MaxConcurrentBatches)
	assert.Equal(500, batchingConfig.FlushPeriodMillis)
}

func TestGetTarget_Silent(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "silent" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with valid stdout config
	tar, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns a valid target and no error
	assert.NotNil(tar)
	assert.Nil(err)

	// Verify the target driver is properly initialized
	assert.NotNil(tar.TargetDriver)

	batchingConfig := tar.GetBatchingConfig()

	// Verify the target has the correct default batching configuration
	assert.Equal(1, batchingConfig.MaxBatchMessages)
	assert.Equal(100000000000, batchingConfig.MaxBatchBytes)
	assert.Equal(100000000000, batchingConfig.MaxMessageBytes)
	assert.Equal(1, batchingConfig.MaxConcurrentBatches)
	assert.Equal(1, batchingConfig.FlushPeriodMillis)
}

func TestGetTarget_InvalidTarget(t *testing.T) {
	assert := assert.New(t)

	// Define HCL config inline as a string
	hclConfig := []byte(`
		target {
			use "fakeHCL" {}
		}
	`)

	c, err := config.NewHclConfig(hclConfig, "test.hcl")
	assert.NoError(err)
	assert.NotNil(c)

	// Call GetTarget with invalid config
	target, err := GetTarget(c.Data.Target, c.Decoder)

	// Assert that it returns nil target and non-nil error
	assert.Nil(target)
	assert.NotNil(err)
	if err != nil {
		assert.Contains(err.Error(), "unknown target")
		assert.Contains(err.Error(), "fakeHCL")
	}
}
