// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	pubsubV1 "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

const (
	pubsubProjectID = `project-test`
)

func initMockPubsubServer() (*pstest.Server, *grpc.ClientConn) {
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8563")

	ctx := context.Background()
	srv := pstest.NewServerWithPort(8563)
	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	_, err = srv.GServer.CreateTopic(ctx, &pubsubV1.Topic{Name: `projects/project-test/topics/test-topic`})
	if err != nil {
		panic(err)
	}

	_, err = srv.GServer.CreateSubscription(ctx, &pubsubV1.Subscription{
		Name:               "projects/project-test/subscriptions/test-sub",
		Topic:              "projects/project-test/topics/test-topic",
		AckDeadlineSeconds: 10,
	})
	if err != nil {
		panic(err)
	}

	return srv, conn
}

func createPubsubResourcesAndWrite() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "0.0.0.0:8432")

	client, err := pubsub.NewClient(ctx, pubsubProjectID)
	if err != nil {
		panic(errors.Wrap(err, "Failed to create PubSub client"))
	}
	defer client.Close()

	topic, err := client.CreateTopic(ctx, `test-topic`)
	if err != nil {
		panic(errors.Wrap(err, "Failed to create pubsub topic"))
	}
	_, err = client.CreateSubscription(ctx, `test-sub`, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
	})
	if err != nil {
		panic(fmt.Errorf("error creating subscription: %v", err))
	}
}

func deletePubsubResources() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "0.0.0.0:8432")

	client, err := pubsub.NewClient(ctx, pubsubProjectID)
	if err != nil {
		panic(errors.Wrap(err, "Failed to create PubSub client"))
	}
	defer client.Close()

	subscription := client.Subscription(`test-sub`)
	err = subscription.Delete(ctx)
	if err != nil {
		panic(errors.Wrap(err, "Failed to delete subscription"))
	}

	topic := client.Topic(`test-topic`)
	if err != nil {
		panic(errors.Wrap(err, "Failed to get topic"))
	}

	err = topic.Delete(ctx)
	if err != nil {
		panic(errors.Wrap(err, "Failed to delete topic"))
	}
}

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestPubSubSource_ReadAndReturnSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	createPubsubResourcesAndWrite()
	defer deletePubsubResources()

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", pubsubProjectID)

	pubsubTarget, err := NewPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(10, "Hello Pubsub!!", nil)

	result, err := pubsubTarget.Write(messages)
	assert.Equal(result.Total(), int64(10))
	assert.Nil(err)
}

func TestPubSubSource_ReadAndReturnSuccessWithMocks(t *testing.T) {
	assert := assert.New(t)

	srv, conn := initMockPubsubServer()
	defer srv.Close()
	defer conn.Close()

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", pubsubProjectID)

	pubsubTarget, err := NewPubSubTarget(`project-test`, `test-topic`)
	assert.NotNil(pubsubTarget)
	assert.Nil(err)
	assert.Equal("projects/project-test/topics/test-topic", pubsubTarget.GetID())
	pubsubTarget.Open()
	defer pubsubTarget.Close()

	messages := testutil.GetTestMessages(10, "Hello Pubsub!!", nil)

	result, err := pubsubTarget.Write(messages)
	assert.Equal(result.Total(), int64(10))
	assert.Nil(err)
}
