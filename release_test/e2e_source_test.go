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

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

func TestE2EPubsubSource(t *testing.T) {
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
	stdOut, cmdErr := runDockerCommand(cmdTemplate, "pubsubSource", configFilePath, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
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
	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "var")

	client := testutil.GetAWSLocalstackSQSClient()

	fmt.Println("Setting up queue")

	queueName := "sqs-queue-e2e"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 10, "Hello SQS!!")
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	fmt.Println("Done setting up queue")

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	stdOut, cmdErr := runDockerCommand(cmdTemplate, "sqsSource", configFilePath, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
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
