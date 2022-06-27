// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package pubsubsource

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	pubsubV1 "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

const (
	pubsubProjectID = `project-test`
)

func initMockPubsubServer() (*pstest.Server, *grpc.ClientConn) {
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8008")
	ctx := context.Background()
	srv := pstest.NewServerWithPort(8008)
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

	numMsgs := 10
	// publish 10 messages
	wg := sync.WaitGroup{}
	for i := 0; i < numMsgs; i++ {
		wg.Add(1)
		go func(i int) {
			_ = srv.Publish(`projects/project-test/topics/test-topic`, []byte("message #"+strconv.Itoa(i)), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
	return srv, conn
}

func createPubsubResourcesAndWrite() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

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

	var wg sync.WaitGroup
	var totalErrors uint64

	numMsgs := 10
	// publish 10 messages
	for i := 0; i < numMsgs; i++ {
		wg.Add(1)
		result := topic.Publish(ctx, &pubsub.Message{
			Data: []byte("message #" + strconv.Itoa(i)),
		})
		go func(i int, res *pubsub.PublishResult) {
			defer wg.Done()
			_, err := res.Get(ctx)
			if err != nil {
				atomic.AddUint64(&totalErrors, 1)
				return
			}
		}(i, result)
	}

	wg.Wait()
}

func deletePubsubResources() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", pubsubProjectID)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

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

func TestPubSubSource_ReadAndReturnSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	createPubsubResourcesAndWrite()
	defer deletePubsubResources()

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", pubsubProjectID)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	output := testutil.ReadAndReturnMessages(pubsubSource, 3*time.Second, testutil.DefaultTestWriteBuilder)
	assert.Equal(len(output), 10)
	for _, message := range output {
		assert.Contains(string(message.Data), `message #`)
		assert.Nil(message.GetError())
	}
	pubsubSource.Stop()
}

func TestPubSubSource_FailToReadIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", pubsubProjectID)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	assert.Panics(func() { testutil.ReadAndReturnMessages(pubsubSource, 3*time.Second, testutil.DefaultTestWriteBuilder) })
	pubsubSource.Stop()
}

func TestPubSubSource_ReadAndReturnSuccessWithMock(t *testing.T) {
	assert := assert.New(t)

	srv, conn := initMockPubsubServer()
	defer srv.Close()
	defer conn.Close()

	t.Setenv("SOURCE_NAME", "pubsub")
	t.Setenv("SOURCE_PUBSUB_SUBSCRIPTION_ID", "test-sub")
	t.Setenv("SOURCE_PUBSUB_PROJECT_ID", pubsubProjectID)

	adaptedHandle := adapterGenerator(configFunction)

	pubsubSourceConfigPair := sourceconfig.ConfigPair{Name: "pubsub", Handle: adaptedHandle}
	supportedSources := []sourceconfig.ConfigPair{pubsubSourceConfigPair}

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects/project-test/subscriptions/test-sub", pubsubSource.GetID())

	output := testutil.ReadAndReturnMessages(pubsubSource, 3*time.Second, testutil.DefaultTestWriteBuilder)
	assert.Equal(len(output), 10)
	pubsubSource.Stop()
}
