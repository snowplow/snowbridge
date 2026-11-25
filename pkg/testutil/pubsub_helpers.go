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

package testutil

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitMockPubsubServer creates a mock PubSub Server for testing
func InitMockPubsubServer(port int, opts []pstest.ServerReactorOption, t *testing.T) (*pstest.Server, *grpc.ClientConn) {
	t.Setenv("PUBSUB_PROJECT_ID", `project-test`)
	t.Setenv(`PUBSUB_EMULATOR_HOST`, fmt.Sprint("localhost:", port))
	ctx := context.Background()
	srv := pstest.NewServerWithPort(port, opts...)
	// Connect to the server without using TLS.
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	_, err = srv.GServer.CreateTopic(ctx, &pubsubpb.Topic{Name: `projects/project-test/topics/test-topic`})
	if err != nil {
		t.Fatal(err)
	}

	_, err = srv.GServer.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               "projects/project-test/subscriptions/test-sub",
		Topic:              "projects/project-test/topics/test-topic",
		AckDeadlineSeconds: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	return srv, conn
}

// CreatePubSubTopicAndSubscription creates a pubsub topic & subscription using the pubsub emulator,
// and returns a publisher and subscriber for them. The topic and subscription are deleted, and the
// client closed, when the test finishes.
func CreatePubSubTopicAndSubscription(t *testing.T, topicName string, subscriptionName string) (*pubsub.Publisher, *pubsub.Subscriber) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	t.Setenv("PUBSUB_PROJECT_ID", `project-test`)
	t.Setenv(`PUBSUB_EMULATOR_HOST`, "localhost:8432")

	client, err := pubsub.NewClient(ctx, `project-test`)
	if err != nil {
		t.Fatal(errors.Wrap(err, "Failed to create PubSub client"))
	}

	topicPath := fmt.Sprintf("projects/%s/topics/%s", client.Project(), topicName)
	subscriptionPath := fmt.Sprintf("projects/%s/subscriptions/%s", client.Project(), subscriptionName)

	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicPath}); err != nil {
		t.Fatal(errors.Wrap(err, "Failed to create pubsub topic"))
	}

	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               subscriptionPath,
		Topic:              topicPath,
		AckDeadlineSeconds: 10,
	}); err != nil {
		t.Fatal(fmt.Errorf("error creating subscription: %v", err))
	}

	t.Cleanup(func() {
		cleanupCtx := context.Background()
		if err := client.SubscriptionAdminClient.DeleteSubscription(cleanupCtx, &pubsubpb.DeleteSubscriptionRequest{Subscription: subscriptionPath}); err != nil {
			t.Logf("error deleting subscription: %v", err)
		}
		if err := client.TopicAdminClient.DeleteTopic(cleanupCtx, &pubsubpb.DeleteTopicRequest{Topic: topicPath}); err != nil {
			t.Logf("error deleting topic: %v", err)
		}
		if err := client.Close(); err != nil {
			t.Logf("error closing PubSub client: %v", err)
		}
	})

	return client.Publisher(topicName), client.Subscriber(subscriptionName)
}

// WriteToPubSubTopic simply writes data to a provided PubSub topic, blocking until all msgs are sent
func WriteToPubSubTopic(t *testing.T, topic *pubsub.Publisher, numMsgs int) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

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

// WriteProvidedDataToPubSubTopic writes the provided data to a provided PubSub topic, blocking until all msgs are sent
func WriteProvidedDataToPubSubTopic(t *testing.T, topic *pubsub.Publisher, data []string) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	var wg sync.WaitGroup
	var totalErrors uint64

	// publish n messages
	for _, msg := range data {
		wg.Add(1)
		result := topic.Publish(ctx, &pubsub.Message{
			Data: []byte(msg),
		})
		go func(res *pubsub.PublishResult) {
			defer wg.Done()
			_, err := res.Get(ctx)
			if err != nil {
				atomic.AddUint64(&totalErrors, 1)
				return
			}
		}(result)
	}

	wg.Wait()
}

// CreatePubsubResourcesAndWrite creates PubSub integration resources, and writes numMsgs
func CreatePubsubResourcesAndWrite(numMsgs int, topicName string, t *testing.T) {
	topic, _ := CreatePubSubTopicAndSubscription(t, topicName, "test-sub")

	WriteToPubSubTopic(t, topic, numMsgs)
}

// ReceiveMessagesFromSubscription receives messages from a PubSub subscription and returns their data
func ReceiveMessagesFromSubscription(t *testing.T, subscription *pubsub.Subscriber) []string {
	ctx, cancel := context.WithTimeout(t.Context(), time.Duration(10)*time.Second)
	defer cancel()

	var mu sync.Mutex
	receivedMessages := make([]string, 0)

	receiveErr := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		receivedMessages = append(receivedMessages, string(msg.Data))
		mu.Unlock()
		msg.Ack()
	})

	if receiveErr != nil && !errors.Is(receiveErr, context.Canceled) {
		t.Fatalf("Unexpected error receiving messages: %v", receiveErr)
	}

	return receivedMessages
}
