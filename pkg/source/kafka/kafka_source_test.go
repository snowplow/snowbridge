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
	assert := assert.New(t)

	topicName := "test-topic"

	// Create mock consumer group with test messages
	mockMessages := generateMockMessages(100, topicName)
	mockConsumer := &mockConsumerGroup{
		messages: mockMessages,
	}

	// Create kafka source using dependency injection
	kafkaSource, err := BuildWithSaramaConsumerInterface(mockConsumer, &kafkaSourceDriver{
		topic:        topicName,
		brokers:      "mock-broker",
		consumerName: "test-consumer",
		log:          logrus.WithField("test", true),
	})
	assert.NotNil(kafkaSource)
	assert.NoError(err)

	// Set up channels
	outputChannel := make(chan *models.Message, 100)

	kafkaSource.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		kafkaSource.Start(ctx)
	})

	// Collect messages with timeout
	successfulReads := testutil.ReadSourceOutput(outputChannel)

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

// Mock implementations for testing
type mockConsumerGroup struct {
	messages  []*mockMessage
	errorChan chan error
	closed    bool
}

type mockMessage struct {
	topic     string
	partition int32
	key       []byte
	value     []byte
	offset    int64
	timestamp time.Time
}

func (m *mockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if m.closed {
		return fmt.Errorf("consumer group is closed")
	}

	// Create mock session
	session := &mockSession{}

	// Call handler Setup
	if err := handler.Setup(session); err != nil {
		return err
	}

	// Create mock claim for each topic
	for _, topic := range topics {
		claim := &mockClaim{
			messages: m.messages,
			topic:    topic,
			msgChan:  make(chan *sarama.ConsumerMessage, len(m.messages)),
		}

		// Populate message channel
		for _, mockMsg := range m.messages {
			if mockMsg.topic == topic {
				claim.msgChan <- &sarama.ConsumerMessage{
					Topic:     mockMsg.topic,
					Partition: mockMsg.partition,
					Key:       mockMsg.key,
					Value:     mockMsg.value,
					Offset:    mockMsg.offset,
					Timestamp: mockMsg.timestamp,
				}
			}
		}
		close(claim.msgChan)

		// Process claim in goroutine
		go func(c *mockClaim) {
			if err := handler.ConsumeClaim(session, c); err != nil {
				logrus.WithError(err).Error("error consuming kafka claim")
				return
			}
		}(claim)
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Call handler Cleanup
	if err := handler.Cleanup(session); err != nil {
		return err
	}

	return ctx.Err()
}

func (m *mockConsumerGroup) Errors() <-chan error {
	if m.errorChan == nil {
		m.errorChan = make(chan error)
	}
	return m.errorChan
}

func (m *mockConsumerGroup) Close() error {
	m.closed = true
	return nil
}

func (m *mockConsumerGroup) Pause(partitions map[string][]int32) {
	// Mock implementation - no-op
}

func (m *mockConsumerGroup) Resume(partitions map[string][]int32) {
	// Mock implementation - no-op
}

func (m *mockConsumerGroup) PauseAll() {
	// Mock implementation - no-op
}

func (m *mockConsumerGroup) ResumeAll() {
	// Mock implementation - no-op
}

type mockSession struct {
	markedMessages []*sarama.ConsumerMessage
}

func (m *mockSession) Claims() map[string][]int32 {
	return map[string][]int32{"test-topic": {0}}
}

func (m *mockSession) MemberID() string {
	return "mock-member"
}

func (m *mockSession) GenerationID() int32 {
	return 1
}

func (m *mockSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	// Mock implementation - no-op
}

func (m *mockSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	// Mock implementation - no-op
}

func (m *mockSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.markedMessages = append(m.markedMessages, msg)
}

func (m *mockSession) Context() context.Context {
	return context.Background()
}

func (m *mockSession) Commit() {
	// Mock implementation - no-op
}

type mockClaim struct {
	messages []*mockMessage
	topic    string
	msgChan  chan *sarama.ConsumerMessage
}

func (m *mockClaim) Topic() string {
	return m.topic
}

func (m *mockClaim) Partition() int32 {
	return 0
}

func (m *mockClaim) InitialOffset() int64 {
	return 0
}

func (m *mockClaim) HighWaterMarkOffset() int64 {
	return int64(len(m.messages))
}

func (m *mockClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.msgChan
}

func generateMockMessages(count int, topic string) []*mockMessage {
	messages := make([]*mockMessage, count)
	for i := range count {
		keyBuf := make([]byte, 0, 16)
		keyBuf = fmt.Appendf(keyBuf, "key-%d", i)

		valueBuf := make([]byte, 0, 8)
		valueBuf = fmt.Append(valueBuf, i)

		messages[i] = &mockMessage{
			topic:     topic,
			partition: 0,
			key:       keyBuf,
			value:     valueBuf,
			offset:    int64(i),
			timestamp: time.Now(),
		}
	}
	return messages
}
