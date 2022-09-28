// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

// TODO: Localstack stuff seems a bit flaky. See if running a more recent version improves things.
// TODO: Is it worth the effort to change all the helpers etc., so that we always use the same input data?
//			// Pro: makes tests more consistent, can remove expected data files
// 			// Pro: Could standardise test cases with other projects
//			// Pro: Same input to everything = one change in one place to update all tests
//			// Con: A fair bit of effort for perhaps not a huge payoff...

func TestE2EPubsubSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-pubsub-source-topic", "e2e-pubsub-source-subscription")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// Write to topic
	testutil.WriteToPubSubTopic(t, topic, 50)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	// Additional env var options allow us to connect to the pubsub emulator
	stdOut, cmdErr := runDockerCommand(cmdTemplate, 3*time.Second, "pubsubSource", configFilePath, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
	}

	expectedFilePath := filepath.Join("cases", "sources", "pubsub", "expected_data.txt")

	data := getDataFromStdoutResult(stdOut)
	evaluateTestCaseString(t, data, expectedFilePath, "PubSub source")
}

// Commented out as it fails due to: https://github.com/snowplow-devops/stream-replicator/issues/215
// We could make this pass if we inored error coming from runDockerCommand,
// but this would hide the genuine issue that sqs produces unnecessary crashes.
/*
func TestE2ESQSSource(t *testing.T) {
if testing.Short() {
	t.Skip("skipping integration test")
}
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	fmt.Println("Setting up queue")

	queueName := "sqs-queue-e2e-source"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 10, "Hello SQS!!")
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	fmt.Println("Done setting up queue")

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	stdOut, cmdErr := runDockerCommand(cmdTemplate, 3*time.Second, "sqsSource", configFilePath, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for SQS source")
		// We seem to keep hitting 'connection reset by peer' error, which kills the job.
		// We're still getting the 10 messages back though. Hard to determine what's causing it...
		// fmt.Println(string(stdOut))

		// Looks like it's related to this:
		// Connection reset is classed as non-retryable as requests may not be idempotent.
		// In our case, requests are idempotent, so we can just instrument a retryer for this.

		// https://github.com/aws/aws-sdk-go/issues/3027#issuecomment-567269161
		// https://github.com/aws/aws-sdk-go/issues/3971
		// https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/request#Retryer
	}

	expectedFilePath := filepath.Join("cases", "sources", "sqs", "expected_data.txt")

	data := getDataFromStdoutResult(stdOut)
	evaluateTestCaseString(t, data, expectedFilePath, "SQS source")
}
*/

func TestE2EKinesisSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	appName := "e2eKinesisSource"

	ddbClient := testutil.GetAWSLocalstackDynamoDBClient()

	fmt.Println("Got client...")
	ddbErr := testutil.CreateAWSLocalstackDynamoDBTables(ddbClient, appName)
	if ddbErr != nil {
		panic(ddbErr)
	}
	defer testutil.DeleteAWSLocalstackDynamoDBTables(ddbClient, appName)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()

	kinErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, appName)
	if kinErr != nil {
		panic(kinErr)
	}

	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, appName)

	putErr := testutil.PutNRecordsIntoKinesis(kinesisClient, 10, appName, "e2e test Kinesis")
	if putErr != nil {
		panic(putErr)
	}

	fmt.Println("Data done")
	fmt.Println("Done setting up resources")

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "kinesis", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	// 3 seconds isn't enough time to wait for this test it seems.
	stdOut, cmdErr := runDockerCommand(cmdTemplate, 10*time.Second, "kinesisSource", configFilePath, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for Kinesis source")
		// TODO: These errors could probably reference the command I guess?
	}

	expectedFilePath := filepath.Join("cases", "sources", "kinesis", "expected_data.txt")

	data := getDataFromStdoutResult(stdOut)
	evaluateTestCaseString(t, data, expectedFilePath, "Kinesis source")
}
