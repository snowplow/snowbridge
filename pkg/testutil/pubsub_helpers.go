// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package testutil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/pkg/errors"
	pubsubV1 "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: Issue here is the port is wrong. Change 'Host' to 'port'.

// InitMockPubsubServer creates a mock PubSub Server for testing
func InitMockPubsubServer(port int, opts []pstest.ServerReactorOption) (*pstest.Server, *grpc.ClientConn) {
	os.Setenv("PUBSUB_PROJECT_ID", `project-test`)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, fmt.Sprint("localhost:", port))
	ctx := context.Background()
	srv := pstest.NewServerWithPort(port, opts...)
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

// CreatePubsubResourcesAndWrite creates PubSub integration resources, and writes numMsgs
func CreatePubsubResourcesAndWrite(numMsgs int) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", `project-test`)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

	client, err := pubsub.NewClient(ctx, `project-test`)
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

	// publish n messages
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

// DeletePubsubResources tears down Pubsub integration resources
func DeletePubsubResources() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	os.Setenv("PUBSUB_PROJECT_ID", `project-test`)
	os.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

	client, err := pubsub.NewClient(ctx, `project-test`)
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
