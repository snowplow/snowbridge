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

	"cloud.google.com/go/pubsub"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

// TODO:
// - Refactor pubsub helpers
// 	//	- We don't need the client to be returned
//	// 	- We can separate the writing of data out
//	//	- We should remove the delete function altogether and just call delete directly on subscription and topic in all the tests, as we do here

func TestE2EPubsubTarget(t *testing.T) {
	assert := assert.New(t)

	_, topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-target-topic", "e2e-target-subscription")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// TODO: should probably use a timed out context here to prevent hangs

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	// Additional env var options allow us to connect to the pubsub emulator
	_, cmdErr := runDockerCommand(cmdTemplate, "pubsubTarget", configFilePath, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
	}

	receiverChannel := make(chan string)

	subReceiver := func(c context.Context, msg *pubsub.Message) {
		fmt.Println(string(msg.Data))
		receiverChannel <- string(msg.Data) // stringify and pass to channel
	}

	subCtx, subCncl := context.WithTimeout(context.Background(), 2*time.Second) //context.WithTimeout
	defer subCncl()

	// Receive data in goroutine
	go subscription.Receive(subCtx, subReceiver)

	var foundData []string

receiveLoop:
	for {
		select {
		case res := <-receiverChannel:
			foundData = append(foundData, res)
		case <-time.After(1 * time.Second):
			break receiveLoop // after 1s with no data, break the loop
		}
	}

	expectedFilePath := filepath.Join("cases", "targets", "pubsub", "expected_data.txt")

	evaluateTestCaseString(t, foundData, expectedFilePath, "PubSub source")
}
