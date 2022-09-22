// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

// Next:
// - Start on sources, use localstack/integration test resources.
// - Factor the helper functions to help this
// - Add tests for gcp asset too.

// 	t.Setenv("PUBSUB_PROJECT_ID", `project-test`)
// t.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

// explanation of arguments:
// -i keeps stdin open
// --mount mounts the config file
// --env sets env var for config file resolution
var cmdTemplatePubSub = `cat %s | docker run -i \
--name srSource \
--net=integration_default \
--mount type=bind,source=%s,target=/config.hcl \
--env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl --env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432 \
snowplow/stream-replicator-aws:` + cmd.AppVersion

// --net=integration_default runs the container on the network that contains our pubsub emulator
// --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432 connects directly to that specific host

// TODO: Should I name these specifically in the docker compose file?

func TestE2EPubsubSource(t *testing.T) {
	assert := assert.New(t)

	testutil.CreatePubsubResourcesAndWrite(50, t)
	defer testutil.DeletePubsubResources(t)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "sources", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	// Goroutine to stop SR after a bit (this can probably be factored nicer)
	go func() {
		time.Sleep(3 * time.Second)
		cmd := exec.Command("bash", "-c", "docker stop srSource")
		// Ensure we print stderr to logs, to make debugging a bit more manageable
		cmd.Stderr = os.Stderr
		cmd.Output()
	}()

	defer func() {
		cmd := exec.Command("bash", "-c", "docker rm srSource") // Remove container, existing stopped container will cause next docker run to fail.

		cmd.Stderr = os.Stderr
		cmd.Output()

	}()

	stdOut, cmdErr := runDockerCommand(cmdTemplatePubSub, configFilePath)
	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
	}

	expectedFilePath := filepath.Join("cases", "sources", "pubsub", "expected_data.txt")

	// TODO: Rename the evaluate function
	evaluateTestCaseTSV(t, stdOut, expectedFilePath, "PubSub source")

}

// TODO: Next steps:
//		// - Factor common stuff out of the transform file and rename approriately

//

// There doesn't seem to be a viable way to do e2e tests for AWS resources without full resources, while adding more value than existing integration tests.
// We could attempt to make using the mock interfaces configurable but this is relatively self-defeating.
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
