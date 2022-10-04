// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

func getSliceFromInput(filepath string) []string {
	inputData, err := os.ReadFile(inputFilePath)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(inputData), "\n")
}

func TestE2EPubsubSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-pubsub-source-topic", "e2e-pubsub-source-subscription")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	dataToSend := getSliceFromInput(inputFilePath)

	for _, binary := range []string{"aws", "gcp"} {

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

func TestE2ESQSSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-e2e-source"
	res, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	dataToSend := getSliceFromInput(inputFilePath)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"aws", "gcp"} {
		testutil.PutProvidedDataIntoSQS(client, *res.QueueUrl, dataToSend)

		stdOut, cmdErr := runDockerCommand(3*time.Second, "sqsSource", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for SQS source")
		}
		data := getDataFromStdoutResult(stdOut)
		evaluateTestCaseString(t, data, inputFilePath, "SQS source "+binary)
	}

}

func TestE2EKinesisSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
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

	dataToSend := getSliceFromInput(inputFilePath)

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
	stdOut, cmdErr := runDockerCommand(5*time.Second, "kinesisSource", configFilePath, "aws", "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error())
	}

	data := getDataFromStdoutResult(stdOut)
	// Output should exactly match input.
	evaluateTestCaseString(t, data, inputFilePath, "Kinesis source aws")
}
