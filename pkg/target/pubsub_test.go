// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/pubsub/pstest"
	"github.com/stretchr/testify/assert"
	pubsubV1 "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestPubSubTarget_WriteSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// Write to topic
	testutil.WriteToPubSubTopic(t, topic, 10)

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(10, "Hello Pubsub!!", nil)

	result, err := pubsubTarget.Write(messages)

	assert.Equal(int64(10), result.Total())
	assert.Equal([]*models.Message(nil), result.Failed)
	assert.Equal([]*models.Message(nil), result.Oversized)

	assert.Nil(err)
}

func TestPubSubTarget_WriteTopicUnopenedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic", "test-sub")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// Write to topic
	testutil.WriteToPubSubTopic(t, topic, 10)

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())

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
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())
	// Write to topic
	testutil.WriteToPubSubTopic(t, topic, 10)

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(1, `test`, nil)
	messages = append(messages, testutil.GetTestMessages(1, ``, nil)...)

	result, err := pubsubTarget.Write(messages)

	assert.Equal(int64(1), result.Total())
	assert.Equal(1, len(result.Invalid))

	assert.Nil(err)
}

// TestPubSubTarget_WriteSuccessWithMocks unit tests the happy path for PubSub target
func TestPubSubTarget_WriteSuccessWithMocks(t *testing.T) {
	assert := assert.New(t)
	srv, conn := testutil.InitMockPubsubServer(8563, nil, t)
	defer srv.Close()
	defer conn.Close()

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)
	// Check that the TargetWriteResult is correct
	assert.Equal(int64(10), twres.SentCount)
	assert.Equal(10, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
	assert.Nil(err)

	res, pullErr := srv.GServer.Pull(context.TODO(), &pubsubV1.PullRequest{
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
	defer srv.Close()
	defer conn.Close()

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)

	// Check that the TargetWriteResult is correct
	assert.Equal(int64(0), twres.SentCount)
	assert.Equal(int64(10), twres.FailedCount)
	assert.Equal(10, len(twres.Failed))
	assert.Nil(twres.Sent)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
	assert.NotNil(err)
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
	srv, conn := testutil.InitMockPubsubServer(8563, opts, t)
	defer srv.Close()
	defer conn.Close()

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetSequentialTestMessages(10, ackFunc)

	twres, err := pubsubTarget.Write(messages)

	// Check that the TargetWriteResult is correct
	assert.Equal(int64(0), twres.SentCount)
	assert.Equal(int64(10), twres.FailedCount)
	assert.Equal(10, len(twres.Failed))
	assert.Nil(twres.Sent)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
	assert.NotNil(err)
	if err != nil {
		assert.True(strings.Contains(err.Error(), "Error writing messages to PubSub topic: 10 errors occurred:"))
		assert.Equal(10, strings.Count(err.Error(), "context deadline exceeded"))
	}
}

// TestNewPubSubTarget_Success tests that we newPubSubTarget returns a PubSubTarget
func TestNewPubSubTarget_Success(t *testing.T) {
	assert := assert.New(t)

	// This isn't needed at present, but adding it as we'll need it after https://github.com/snowplow/snowbridge/issues/151
	srv, conn := testutil.InitMockPubsubServer(8563, nil, t)
	defer srv.Close()
	defer conn.Close()

	pubsubTarget, err := newPubSubTarget(`project-test`, `test-topic`)

	assert.Nil(err)
	assert.NotNil(pubsubTarget)
	assert.IsType(PubSubTarget{}, *pubsubTarget)
}

// TestnewPubSubTarget_Failure tests that we fail early when we cannot reach pubsub
// Commented out as this behaviour is not currently instrumented.
// This test serves to illustrate the desired behaviour for this issue: https://github.com/snowplow/snowbridge/issues/151
/*
func TestnewPubSubTarget_Failure(t *testing.T) {
	assert := assert.New(t)

	pubsubTarget, err := newPubSubTarget(`nonexistent-project`, `nonexistent-topic`)

	// TODO: Test for the actual error we expect, when we have instrumented failing fast
	assert.NotNil(err)
	assert.Nil(pubsubTarget)
}
*/
