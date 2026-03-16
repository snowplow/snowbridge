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

package releasetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"

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
	t.Run("kafka-sasl", testE2EKafkaSourceSASL)
}

func getSliceFromInput(filepath string) []string {
	inputData, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(inputData), "\n")
}

var dataToSend = getSliceFromInput(inputFilePath)

func testE2EPubsubSource(t *testing.T) {
	testCases := []struct {
		name         string
		binary       string
		topic        string
		subscription string
		configFile   string
	}{
		{name: "default", binary: "", topic: "e2e-pubsub-source-topic", subscription: "e2e-pubsub-source-subscription", configFile: "config.hcl"},
		{name: "aws-only", binary: "-aws-only", topic: "e2e-pubsub-source-topic-aws-only", subscription: "e2e-pubsub-source-subscription-aws-only", configFile: "config_aws_only.hcl"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			// Create topic and subscription
			topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, testCase.topic, testCase.subscription)
			defer func() {
				if err := topic.Delete(t.Context()); err != nil {
					logrus.Error(err.Error())
				}
			}()
			defer func() {
				if err := subscription.Delete(t.Context()); err != nil {
					logrus.Error(err.Error())
				}
			}()

			configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "pubsub", testCase.configFile))
			if err != nil {
				panic(err)
			}

			testutil.WriteProvidedDataToPubSubTopic(t, topic, dataToSend)

			// Additional env var options allow us to connect to the pubsub emulator
			stdOut, cmdErr := runDockerCommand(10*time.Second, "pubsubSource", configFilePath, testCase.binary, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
			}

			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, inputFilePath, "PubSub source "+testCase.binary)
		})
	}
}

func testE2ESQSSource(t *testing.T) {
	testCases := []struct {
		name       string
		binary     string
		queueName  string
		configFile string
	}{
		{name: "default", binary: "", queueName: "sqs-queue-e2e-source", configFile: "config.hcl"},
		{name: "aws-only", binary: "-aws-only", queueName: "sqs-queue-e2e-source-aws-only", configFile: "config_aws_only.hcl"},
	}

	client := testutil.GetAWSLocalstackSQSClient()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			res, err := testutil.CreateAWSLocalstackSQSQueue(client, testCase.queueName)
			if err != nil {
				panic(err)
			}

			configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "sqs", testCase.configFile))
			if err != nil {
				panic(err)
			}

			testutil.PutProvidedDataIntoSQS(client, *res.QueueUrl, dataToSend)

			stdOut, cmdErr := runDockerCommand(10*time.Second, "sqsSource", configFilePath, testCase.binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error(), "Docker run returned error for SQS source")
			}
			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, inputFilePath, "SQS source "+testCase.binary)
		})
	}
}

func testE2EKinesisSource(t *testing.T) {
	testCases := []struct {
		name       string
		binary     string
		appName    string
		configFile string
	}{
		// Kinesis source may only use the aws binary
		{name: "aws-only", binary: "-aws-only", appName: "e2eKinesisSource", configFile: "config.hcl"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			ddbClient := testutil.GetAWSLocalstackDynamoDBClient()

			ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(ddbClient, testCase.appName)
			if ddbErr != nil {
				panic(ddbErr)
			}
			defer func() {
				if err := testutil.DeleteAWSLocalstackDynamoDBTables(ddbClient, testCase.appName); err != nil {
					logrus.Error(err.Error())
				}
			}()

			kinesisClient := testutil.GetAWSLocalstackKinesisClient()

			kinErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, testCase.appName, 1)
			if kinErr != nil {
				panic(kinErr)
			}
			defer func() {
				if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, testCase.appName); err != nil {
					logrus.Error(err.Error())
				}
			}()

			putErr := testutil.PutProvidedDataIntoKinesis(kinesisClient, testCase.appName, dataToSend)
			if putErr != nil {
				panic(putErr)
			}

			configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "kinesis", testCase.configFile))
			if err != nil {
				panic(err)
			}

			// Since setup is slower for kinesis source, if this test is flaky we may need to add more time here
			stdOut, cmdErr := runDockerCommand(5*time.Second, "kinesisSource", configFilePath, testCase.binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error(), "Docker run returned error for Kinesis source")
			}

			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, inputFilePath, "Kinesis source "+testCase.binary)
		})
	}
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
	defer func() {
		if err := adminClient.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			err2 := adminClient.CreateTopic(testCase.topic,
				&sarama.TopicDetail{NumPartitions: 1,
					ReplicationFactor: 1}, false)
			if err2 != nil {
				panic(err2)
			}
			defer func() {
				if err := adminClient.DeleteTopic(testCase.topic); err != nil {
					logrus.Error(err.Error())
				}
			}()

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

func testE2EKafkaSourceSASL(t *testing.T) {
	testCases := []struct {
		name       string
		binary     string
		topic      string
		configFile string
	}{
		{name: "default", binary: "", topic: "e2e-kafka-sasl-source", configFile: "config.hcl"},
		{name: "aws-only", binary: "-aws-only", topic: "e2e-kafka-sasl-source-aws-only", configFile: "config-aws-only.hcl"},
	}

	// Create SASL config for admin client
	saslConfig := sarama.NewConfig()
	saslConfig.Net.SASL.Enable = true
	saslConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	saslConfig.Net.SASL.User = "testuser"
	saslConfig.Net.SASL.Password = "testuser-password"
	saslConfig.Net.SASL.Version = 0
	saslConfig.ApiVersionsRequest = false

	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9093"}, saslConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert := assert.New(t)

			err2 := adminClient.CreateTopic(testCase.topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
			if err2 != nil {
				panic(err2)
			}
			defer func() {
				if err := adminClient.DeleteTopic(testCase.topic); err != nil {
					logrus.Error(err.Error())
				}
			}()

			configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "kafka-sasl", testCase.configFile))
			if err != nil {
				panic(err)
			}

			// Create producer with SASL
			producerConfig := sarama.NewConfig()
			producerConfig.Net.SASL.Enable = true
			producerConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			producerConfig.Net.SASL.User = "testuser"
			producerConfig.Net.SASL.Password = "testuser-password"
			producerConfig.Net.SASL.Version = 0
			producerConfig.ApiVersionsRequest = false
			producerConfig.Producer.Return.Successes = true
			producerConfig.Producer.Return.Errors = true

			producer, producerError := sarama.NewSyncProducer(strings.Split("localhost:9093", ","), producerConfig)
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

			stdOut, cmdErr := runDockerCommand(10*time.Second, "kafkaSourceSASL", configFilePath, testCase.binary, "")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error(), "Docker run returned error for Kafka SASL source")
			}
			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, inputFilePath, "Kafka SASL source "+testCase.binary)
		})
	}
}
