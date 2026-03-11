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

package pubsub

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/pubsub/pstest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	// nolint: govet,staticcheck
	pubsubV1 "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestPubSubTarget_WriteSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	assert.Nil(err)

	err = pubsubTarget.Open()
	assert.Nil(err)
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(5, "Hello Pubsub!!", nil)

	result, err := pubsubTarget.Write(messages)

	assert.Nil(err)
	assert.Equal(5, len(result.Sent))
	assert.Equal(0, len(result.Failed))
	assert.Equal([]*models.Message(nil), result.Failed)

	// Receive messages from subscription to verify they landed in the topic
	receivedMessages := testutil.ReceiveMessagesFromSubscription(t, subscription)

	assert.Equal(5, len(receivedMessages))
	for _, receivedMsg := range receivedMessages {
		assert.Equal("Hello Pubsub!!", receivedMsg)
	}
}

func TestPubSubTarget_WriteTopicUnopenedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	assert.Nil(err)

	messages := testutil.GetTestMessages(1, ``, nil)

	_, err = pubsubTarget.Write(messages)

	assert.Error(err)
}

func TestPubSubTarget_WithInvalidMessageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	assert.Nil(err)

	err = pubsubTarget.Open()
	assert.Nil(err)
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(1, `test`, nil)
	messages = append(messages, testutil.GetTestMessages(1, ``, nil)...)

	result, err := pubsubTarget.Write(messages)

	assert.Nil(err)
	assert.Equal(1, len(result.Sent))
	assert.Equal(0, len(result.Failed))
	assert.Equal(1, len(result.Invalid))

	// Receive messages from subscription to verify they landed in the topic
	receivedMessages := testutil.ReceiveMessagesFromSubscription(t, subscription)

	assert.Equal(1, len(receivedMessages))
	assert.Equal("test", receivedMessages[0])
}

// TestPubSubTarget_WriteSuccessWithMocks unit tests the happy path for PubSub target
func TestPubSubTarget_WriteSuccessWithMocks(t *testing.T) {
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	err = pubsubTarget.Open()
	assert.Nil(err)

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)
	// Check that the TargetWriteResult is correct
	assert.Nil(err)
	assert.Equal(10, len(twres.Sent))
	assert.Equal(0, len(twres.Failed))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Invalid)

	// nolint: staticcheck
	res, pullErr := srv.GServer.Pull(t.Context(), &pubsubV1.PullRequest{
		Subscription: "projects/project-test/subscriptions/test-sub",
		MaxMessages:  15, // 15 max messages to ensure we don't miss dupes
	})
	if pullErr != nil {
		t.Fatal(pullErr)
	}

	var results []string

	for _, msg := range res.ReceivedMessages {
		results = append(results, string(msg.Message.Data))
	}

	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sort.Strings(results)
	assert.Equal(expected, results)

	// Check that we acked correct amount of times
	assert.Equal(int64(10), ackOps)
}

// TestPubSubTarget_WriteFailureWithMocks unit tests the unhappy path for PubSub target
func TestPubSubTarget_WriteFailureWithMocks(t *testing.T) {
	assert := assert.New(t)

	// Initialise the mock server with un-retryable error
	opts := []pstest.ServerReactorOption{
		pstest.WithErrorInjection("Publish", codes.PermissionDenied, "Some Error"),
	}
	srv, conn := testutil.InitMockPubsubServer(8563, opts, t)
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	if err != nil {
		t.Fatal(err)
	}
	err = pubsubTarget.Open()
	assert.Nil(err)
	defer pubsubTarget.Close()

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)

	// Check that the TargetWriteResult is correct
	assert.NotNil(err)
	assert.Equal(0, len(twres.Sent))
	assert.Equal(10, len(twres.Failed))
	assert.Nil(twres.Sent)
	assert.Nil(twres.Invalid)
	if err != nil {
		assert.True(strings.Contains(err.Error(), "Error writing messages to PubSub topic: 10 errors occurred:"))
		assert.Equal(10, strings.Count(err.Error(), "rpc error: code = PermissionDenied desc = Some Error"))
	}
}

// TestPubSubTarget_WriteFailureRetryableWithMocks unit tests the unhappy path for PubSub target
// This isn't an integration test, but takes a long time so we skip on short runs
// This test demonstrates the case where retryable errors are obscured somewhat.
// We should try to make these more transparent: https://github.com/snowplow/snowbridge/issues/156
func TestPubSubTarget_WriteFailureRetryableWithMocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test")
	}
	assert := assert.New(t)

	// Initialise the mock server with retryable error
	opts := []pstest.ServerReactorOption{
		pstest.WithErrorInjection("Publish", codes.Unknown, "Some Error"),
	}
	srv, conn := testutil.InitMockPubsubServer(8564, opts, t)
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")
	assert.NotNil(pubsubTarget)
	if err != nil {
		t.Fatal(err)
	}
	err = pubsubTarget.Open()
	assert.Nil(err)
	defer pubsubTarget.Close()

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)

	// Check that the TargetWriteResult is correct
	assert.NotNil(err)
	assert.Equal(0, len(twres.Sent))
	assert.Equal(10, len(twres.Failed))
	assert.Nil(twres.Sent)
	assert.Nil(twres.Invalid)
	if err != nil {
		assert.True(strings.Contains(err.Error(), "Error writing messages to PubSub topic: 10 errors occurred:"))
		assert.Equal(10, strings.Count(err.Error(), "context deadline exceeded"))
	}
}

// TestNewPubSubTarget_Success tests that we can create a PubSubTargetDriver
func TestNewPubSubTarget_Success(t *testing.T) {
	assert := assert.New(t)

	// This isn't needed at present, but adding it as we'll need it after https://github.com/snowplow/snowbridge/issues/151
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

	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")

	assert.Nil(err)
	assert.NotNil(pubsubTarget)
	assert.IsType(PubSubTargetDriver{}, *pubsubTarget)
}

// TestnewPubSubTarget_Failure tests that we fail early when we cannot reach pubsub
// Commented out as this behaviour is not currently instrumented.
// This test serves to illustrate the desired behaviour for this issue: https://github.com/snowplow/snowbridge/issues/151
/*
func TestnewPubSubTarget_Failure(t *testing.T) {
	assert := assert.New(t)

	pubsubTarget, err := newPubSubTarget(`nonexistent-project`, `nonexistent-topic`, "")

	// TODO: Test for the actual error we expect, when we have instrumented failing fast
	assert.NotNil(err)
	assert.Nil(pubsubTarget)
}
*/

func TestPubSubTargetDriver_Batcher(t *testing.T) {
	driver := &PubSubTargetDriver{}
	defaultConfig := driver.GetDefaultConfiguration().(*PubSubTargetConfig)
	driver.BatchingConfig = *defaultConfig.BatchingConfig

	t.Run("adding 100th message triggers send with empty new batch", func(t *testing.T) {
		smallMessages := testutil.GetTestMessages(99, "small", nil)
		currentBatchDataBytes := 0
		for _, msg := range smallMessages {
			currentBatchDataBytes += len(msg.Data)
		}

		currentBatch := targetiface.CurrentBatch{
			Messages:  smallMessages,
			DataBytes: currentBatchDataBytes,
		}
		additionalMessage := testutil.GetTestMessages(1, "small", nil)[0]

		batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, additionalMessage)

		assert.Len(t, batchToSend, 100, "Should send complete batch of 100 messages")
		assert.Len(t, newCurrentBatch.Messages, 0, "Should have empty current batch after sending")
		assert.Equal(t, 0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")
		assert.Nil(t, oversized, "Should have no oversized message")
	})

	t.Run("oversized message is returned as oversized with no batch sent", func(t *testing.T) {
		oversizedMessage := testutil.GetTestMessages(1, testutil.GenRandomString(1_148_5760), nil)[0]

		emptyBatch := targetiface.CurrentBatch{}

		batchToSend, newCurrentBatch, oversized := driver.Batcher(emptyBatch, oversizedMessage)

		assert.Nil(t, batchToSend, "Should not send any batch for oversized message")
		assert.Len(t, newCurrentBatch.Messages, 0, "Current batch should remain empty")
		assert.Equal(t, 0, newCurrentBatch.DataBytes, "Current batch bytes should remain 0")
		assert.NotNil(t, oversized, "Should return oversized message")
		assert.Equal(t, oversizedMessage, oversized, "Should return the exact oversized message")
	})
}

// TestPubSubTargetDriver_InitFromConfig_WithCredentials tests initialization with credentials
func TestPubSubTargetDriver_InitFromConfig_WithCredentials(t *testing.T) {
	assert := assert.New(t)

	// This will fail because the credentials file doesn't exist
	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "/path/to/nonexistent-creds.json")

	assert.Nil(pubsubTarget)
	assert.NotNil(err)
	assert.Contains(err.Error(), "Failed to create PubSub client")
}

// TestPubSubTargetConfigFunction_WithoutCredentials tests the config function using default credentials
func TestPubSubTargetConfigFunction_WithoutCredentials(t *testing.T) {
	assert := assert.New(t)

	// This might succeed or fail depending on the environment's default credentials
	// We just test that the function runs without panicking
	pubsubTarget, err := newTestPubSubTargetDriver(`project-test`, `test-topic`, "")

	// Either succeeds with default credentials or fails gracefully
	if err != nil {
		assert.Nil(pubsubTarget)
		assert.Contains(err.Error(), "Failed to create PubSub client")
	} else {
		assert.NotNil(pubsubTarget)
	}
}

func newTestPubSubTargetDriver(projectID, topicName, credentialsPath string) (*PubSubTargetDriver, error) {
	driver := &PubSubTargetDriver{}

	c := driver.GetDefaultConfiguration()
	cfg, ok := c.(*PubSubTargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type")
	}

	cfg.ProjectID = projectID
	cfg.TopicName = topicName
	cfg.CredentialsPath = credentialsPath

	err := driver.InitFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return driver, nil
}
