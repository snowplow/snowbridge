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

package kafkasource

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfiguration(t *testing.T) {
	cfg := DefaultConfiguration()

	assert.Equal(t, "range", cfg.Assignor)
	assert.Equal(t, "sha512", cfg.SASLAlgorithm)
	assert.False(t, cfg.EnableTLS)
}

func TestKafkaSource_StartSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up Kafka resources
	admin, err := testutil.GetKafkaAdminClient()
	if err != nil {
		t.Fatalf("Failed to create Kafka admin client: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	producer, err := testutil.GetKafkaSyncProducer()
	if err != nil {
		t.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	topicName := fmt.Sprintf("kafka-source-success-%d", time.Now().Unix())
	createErr := testutil.CreateKafkaTopic(admin, topicName, 1, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if err := testutil.DeleteKafkaTopic(admin, topicName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put 100 messages into Kafka topic
	var messages []string
	for i := 0; i < 100; i++ {
		messages = append(messages, strconv.Itoa(i))
	}
	putErr := testutil.PutProvidedDataIntoKafka(producer, topicName, messages)
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second)

	// Create the source
	cfg := DefaultConfiguration()
	cfg.TopicName = topicName
	cfg.Brokers = testutil.KafkaBrokerEndpoint
	cfg.ConsumerName = "test-consumer-success"
	cfg.OffsetsInitial = sarama.OffsetOldest

	kafkaSource, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(kafkaSource)

	// Set up channels
	outputChannel := make(chan *models.Message, 100)

	kafkaSource.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		kafkaSource.Start(ctx)
	})

	// Collect messages with timeout
	var successfulReads []*models.Message
	for i := 0; i < 5 && len(successfulReads) == 0; i++ {
		successfulReads = testutil.ReadSourceOutput(outputChannel)
	}

	assert.Equal(100, len(successfulReads))

	cancel()

	// Ack all messages
	for _, msg := range successfulReads {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	assert.True(common.WaitWithTimeout(&wg, 5*time.Second), "Source is not finished even though it has been stopped and all messages have been acked")

	// Check that there are no errors in the results and make a slice of the data converted to int
	var found []int
	for _, message := range successfulReads {
		intVal, _ := strconv.Atoi(string(message.Data))
		found = append(found, intVal)
	}

	// Check for uniqueness
	sort.Ints(found)
	for i, valFound := range found {
		assert.Equal(i, valFound)
	}
}

// TestKafkaSource_AtLeastOnce verifies that:
// 1. The kafkaOffsetSequencer prevents message loss when messages are acked out of order
// 2. Offsets are only committed when all previous messages have been acked (sequential ordering)
// 3. At-least-once delivery semantics are maintained even with concurrent processing
func TestKafkaSource_AtLeastOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up Kafka resources
	admin, err := testutil.GetKafkaAdminClient()
	if err != nil {
		t.Fatalf("Failed to create Kafka admin client: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	producer, err := testutil.GetKafkaSyncProducer()
	if err != nil {
		t.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	topicName := fmt.Sprintf("kafka-source-restart-%d", time.Now().Unix())
	createErr := testutil.CreateKafkaTopic(admin, topicName, 1, 1)
	if createErr != nil {
		t.Fatal(createErr)
	}
	defer func() {
		if err := testutil.DeleteKafkaTopic(admin, topicName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Put ten records into Kafka topic
	messages := []string{
		"msg-0", "msg-1", "msg-2", "msg-3", "msg-4",
		"msg-5", "msg-6", "msg-7", "msg-8", "msg-9",
	}
	putErr := testutil.PutProvidedDataIntoKafka(producer, topicName, messages)
	if putErr != nil {
		t.Fatal(putErr)
	}

	time.Sleep(1 * time.Second)

	// Create the source
	cfg := DefaultConfiguration()
	cfg.TopicName = topicName
	cfg.Brokers = testutil.KafkaBrokerEndpoint
	cfg.ConsumerName = "test-consumer-restart"
	cfg.OffsetsInitial = sarama.OffsetOldest

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

	var firstBatch []*models.Message
	for i := 0; i < 5 && len(firstBatch) == 0; i++ {
		firstBatch = testutil.ReadSourceOutput(outputChannel)
	}

	assert.Equal(10, len(firstBatch))
	assert.Equal("msg-0", string(firstBatch[0].Data))
	assert.Equal("msg-1", string(firstBatch[1].Data))
	assert.Equal("msg-2", string(firstBatch[2].Data))
	assert.Equal("msg-3", string(firstBatch[3].Data))
	assert.Equal("msg-4", string(firstBatch[4].Data))
	assert.Equal("msg-5", string(firstBatch[5].Data))
	assert.Equal("msg-6", string(firstBatch[6].Data))
	assert.Equal("msg-7", string(firstBatch[7].Data))
	assert.Equal("msg-8", string(firstBatch[8].Data))
	assert.Equal("msg-9", string(firstBatch[9].Data))

	// Out-of-order acking!
	// Let's imagine concurrent processing and that only msg-5 is successful.
	// Messages 0-4 fail, are never acked explicitly,
	// This call doesn't block.
	firstBatch[5].AckFunc()

	cancel()

	// Kafka consumer should quit after context cancellation
	assert.True(common.WaitWithTimeout(&wg, 30*time.Second))

	time.Sleep(5 * time.Second)

	// Restarting source...
	secondSource, err := BuildFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(secondSource)

	outputChannel = make(chan *models.Message, 10)

	secondSource.SetChannels(outputChannel)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	wg.Go(func() {
		secondSource.Start(ctx)
	})

	var secondBatch []*models.Message
	for i := 0; i < 5 && len(secondBatch) == 0; i++ {
		secondBatch = testutil.ReadSourceOutput(outputChannel)
	}

	// With kafkaOffsetSequencer: We only acked msg-5 (out-of-order acking).
	// The sequencer ensures msg-5's ack WAITS for msg-0 through msg-4 to be acked first.
	// Since msg-0 through msg-4 are never acked, msg-5's ack never executes.
	// Result: No offset is committed, all 10 messages are redelivered (at-least-once delivery).
	assert.Equal(10, len(secondBatch))
	assert.Equal("msg-0", string(secondBatch[0].Data))
	assert.Equal("msg-1", string(secondBatch[1].Data))
	assert.Equal("msg-2", string(secondBatch[2].Data))
	assert.Equal("msg-3", string(secondBatch[3].Data))
	assert.Equal("msg-4", string(secondBatch[4].Data))
	assert.Equal("msg-5", string(secondBatch[5].Data))
	assert.Equal("msg-6", string(secondBatch[6].Data))
	assert.Equal("msg-7", string(secondBatch[7].Data))
	assert.Equal("msg-8", string(secondBatch[8].Data))
	assert.Equal("msg-9", string(secondBatch[9].Data))
}
