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

// TODO: Next steps:
// - Add a config option to use localstack endpoint
//

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

// TODO: Circle back to AWS stuff after adding option to change endpoint
/*
func TestE2ESQSSource(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "var")

	// Won't resolve...
	// https://docs.aws.amazon.com/sdk-for-go/api/aws/endpoints/#hdr-Using_Custom_Endpoints
	// Might need to use that...

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

	stdOut, cmdErr := runDockerCommand(configFilePath)

	fmt.Println(stdOut)
	fmt.Println(cmdErr)

	assert.Nil(nil)

}
*/
