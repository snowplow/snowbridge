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

package pubsubsource

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/pstest"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

// TestPubSubSource_ReadAndReturnSuccess verifies basic pubsub source functionality:
// 1. Messages are successfully read from the pubsub subscription
// 2. All published messages are received by the source
// 3. Messages are properly acked after processing
// 4. The source respects context cancellation and shuts down gracefully
// 5. The output channel is properly closed after shutdown
func TestPubSubSource_ReadAndReturnSuccess(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8008, nil, t)
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

	// Publish ten messages
	publishMessages(srv, 10, "")

	// Create the source source using BuildFromConfig
	cfg := DefaultConfiguration()
	cfg.ProjectID = "project-test"
	cfg.SubscriptionID = "test-sub"

	source, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(10, len(successfulReads))

	cancel()

	// Check that we got exactly the 10 messages we want, with no duplicates
	msgDatas := make([]string, 0)
	for _, msg := range successfulReads {
		msgDatas = append(msgDatas, string(msg.Data))
		msg.AckFunc()
	}
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sort.Strings(msgDatas)
	assert.Equal(expected, msgDatas)

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

// TestPubSubSource_WaitForDelayedAcks verifies that:
// 1. The source properly handles slow message processing without timing out
// 2. Messages can take time to be acked/nacked without causing issues
func TestPubSubSource_WaitForDelayedAcks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Create topic and subscription
	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "test-topic-ack", "test-sub-ack")
	defer func() {
		if err := topic.Delete(context.Background()); err != nil {
			logrus.Error(err.Error())
		}
	}()
	defer func() {
		if err := subscription.Delete(context.Background()); err != nil {
			logrus.Error(err.Error())
		}
	}()

	testutil.WriteToPubSubTopic(t, topic, 10)

	cfg := DefaultConfiguration()
	cfg.ProjectID = "project-test"
	cfg.SubscriptionID = "test-sub-ack"

	source, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(10, len(successfulReads))

	cancel()

	assert.False(common.WaitWithTimeout(&wg, 1*time.Second), "Source finished even though it still waits for acks/nacks")

	// Ack first half...
	for _, msg := range successfulReads[0:5] {
		msg.AckFunc()
	}

	time.Sleep(2 * time.Second)

	assert.False(common.WaitWithTimeout(&wg, 1*time.Second), "Source finished even though not all acks/nacks happened yet")

	// and nack the other half...
	for _, msg := range successfulReads[5:10] {
		msg.NackFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

// TestPubSubSource_SourceRestart verifies that:
// 1. When a source is restarted after processing and acking/nacking messages, it handles the restart correctly
// 2. The source processes new messages published after the first run
// 3. Messages that were nacked in the first run are redelivered and can be processed again
func TestPubSubSource_SourceRestart(t *testing.T) {
	assert := assert.New(t)

	srv, conn := testutil.InitMockPubsubServer(8008, nil, t)
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

	// Publish ten messages
	publishMessages(srv, 10, "msg-")

	// Create the source
	cfg := DefaultConfiguration()
	cfg.ProjectID = "project-test"
	cfg.SubscriptionID = "test-sub"

	source, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(source)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)
	assert.Equal(10, len(successfulReads))

	cancel()

	// Ack 5 messages from the first batch...
	for _, msg := range successfulReads[0:5] {
		msg.AckFunc()
	}

	// And nack the other 5 messages...
	for _, msg := range successfulReads[5:10] {
		msg.NackFunc()
	}

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")

	// Second batch! Publish new messages
	publishMessages(srv, 10, "second-run-msg-")

	// Build another source (simulating app restart) and confirm it only consumes unacked messages
	secondSource, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(secondSource)

	outputChannel = make(chan *models.Message, 10)

	secondSource.SetChannels(outputChannel)

	ctx, cancel = context.WithCancel(context.Background())
	wg.Go(func() {
		secondSource.Start(ctx)
	})

	// Eventually we should have 5 nacked from the first batch + 10 from the second batch, so 15 total
	successfulReads = make([]*models.Message, 0)
	for i := 0; i < 5; i++ {
		successfulReads = append(successfulReads, testutil.ReadSourceOutput(outputChannel)...)
		for _, msg := range successfulReads {
			msg.AckFunc()
		}
		if len(successfulReads) > 14 {
			break
		}
	}
	assert.Equal(15, len(successfulReads))

	cancel()

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second), "Source is not finished even though it has been stopped and all messages have been acked/nacked")

	_, ok = <-outputChannel
	assert.False(ok, "Output channel should be closed")
}

func publishMessages(srv *pstest.Server, numMsgs int, prefix string) {
	wg := sync.WaitGroup{}
	for i := 0; i < numMsgs; i++ {
		wg.Add(1)
		go func(i int) {
			_ = srv.Publish(`projects/project-test/topics/test-topic`, []byte(prefix+strconv.Itoa(i)), nil)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
