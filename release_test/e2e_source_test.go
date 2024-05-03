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

package releasetest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/pkg/testutil"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestE2ESources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Run("pubsub", testE2EPubsubSource)
	t.Run("sqs", testE2ESQSSource)
	t.Run("kinesis", testE2EKinesisSource)
	t.Run("kafka", testE2EKafkaSource)
}

func getSliceFromInput(filepath string) []string {
	inputData, err := os.ReadFile(inputFilePath)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(inputData), "\n")
}

var dataToSend = getSliceFromInput(inputFilePath)

func testE2EPubsubSource(t *testing.T) {
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-pubsub-source-topic", "e2e-pubsub-source-subscription")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {

		testutil.WriteProvidedDataToPubSubTopic(t, topic, dataToSend)

		// Additional env var options allow us to connect to the pubsub emulator
		stdOut, cmdErr := runDockerCommand(3*time.Second, "pubsubSource", configFilePath, binary, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error())
		}

		data := getDataFromStdoutResult(stdOut)
		// Output should exactly match input.
		evaluateTestCaseString(t, data, inputFilePath, "PubSub source "+binary)
	}

}

func testE2ESQSSource(t *testing.T) {
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-e2e-source"
	res, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {
		testutil.PutProvidedDataIntoSQS(client, *res.QueueUrl, dataToSend)

		stdOut, cmdErr := runDockerCommand(3*time.Second, "sqsSource", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for SQS source")
		}
		data := getDataFromStdoutResult(stdOut)
		evaluateTestCaseString(t, data, inputFilePath, "SQS source "+binary)
	}

}

func testE2EKinesisSource(t *testing.T) {
	assert := assert.New(t)

	appName := "e2eKinesisSource"

	ddbClient := testutil.GetAWSLocalstackDynamoDBClient()

	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(ddbClient, appName)
	if ddbErr != nil {
		panic(ddbErr)
	}
	defer testutil.DeleteAWSLocalstackDynamoDBTables(ddbClient, appName)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()

	kinErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, appName, 1)
	if kinErr != nil {
		panic(kinErr)
	}

	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, appName)

	putErr := testutil.PutProvidedDataIntoKinesis(kinesisClient, appName, dataToSend)
	if putErr != nil {
		panic(putErr)
	}

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "kinesis", "config.hcl"))
	if err != nil {
		panic(err)
	}

	// Kinesis source may only use the aws binary

	// Since setup is slower for kinesis source, if this test is flaky we may need to add more time here
	stdOut, cmdErr := runDockerCommand(5*time.Second, "kinesisSource", configFilePath, "-aws-only", "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error())
	}

	data := getDataFromStdoutResult(stdOut)
	// Output should exactly match input.
	evaluateTestCaseString(t, data, inputFilePath, "Kinesis source aws")
}

func testE2EKafkaSource(t *testing.T) {
	testCases := []struct {
		name       string
		binary     string
		topic      string
		configFile string
	}{
		{name: "default", binary: "", topic: "e2e-kafka-source", configFile: "config.hcl"},
		{name: "aws-only", binary: "-aws-only", topic: "e2e-kafka-source-aws-only",
			configFile: "config-aws-only.hcl"},
	}
	// We use localhost:9092 here as we're running from host machine.
	// The address in our Snowbridge config is different ("broker:29092"), since they're on the shared docker network.
	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer adminClient.Close()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			err2 := adminClient.CreateTopic(testCase.topic,
				&sarama.TopicDetail{NumPartitions: 1,
					ReplicationFactor: 1}, false)
			if err2 != nil {
				panic(err2)
			}
			defer adminClient.DeleteTopic(testCase.topic)

			configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "kafka", testCase.configFile))
			if err != nil {
				panic(err)
			}

			saramaConfig := sarama.NewConfig()
			// Must be enabled for the SyncProducer
			saramaConfig.Producer.Return.Successes = true
			saramaConfig.Producer.Return.Errors = true
			producer, producerError := sarama.NewSyncProducer(strings.Split("localhost:9092", ","), saramaConfig)
			if producerError != nil {
				panic(producerError)
			}

			for _, data := range dataToSend {
				_, _, sendMessageErr := producer.SendMessage(&sarama.ProducerMessage{
					Topic: testCase.topic,
					Value: sarama.StringEncoder(data),
				})
				if sendMessageErr != nil {
					panic(sendMessageErr)
				}
			}

			stdOut, cmdErr := runDockerCommand(5*time.Second, "kafkaSource", configFilePath,
				testCase.binary,
				"--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error(), "Docker run returned error for Kafka source")
			}
			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, inputFilePath, "Kafka source "+testCase.binary)
		})
	}
}
