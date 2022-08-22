// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kafkasource

import (
	"bytes"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
	"github.com/twinj/uuid"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"github.com/stretchr/testify/assert"
)

const (
	targetErr = `target error`
	closeErr  = `close error`
)

type sessionMock struct{}

func (s sessionMock) Claims() map[string][]int32 {
	return nil
}

func (s sessionMock) MemberID() string {
	return ``
}

func (s sessionMock) GenerationID() int32 {
	return 0
}

func (s sessionMock) MarkOffset(topic string, partition int32, offset int64, metadata string) {}

func (s sessionMock) Commit() {}

func (s sessionMock) ResetOffset(topic string, partition int32, offset int64, metadata string) {}

func (s sessionMock) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {}

func (s sessionMock) Context() context.Context {
	return nil
}

// claimMock mocks a ConsumerGroupClaim
type claimMock struct {
	sarama.ConsumerGroupClaim
	messages []*sarama.ConsumerMessage
}

// Messages fills a channel with the claim messages and returns it
func (c claimMock) Messages() <-chan *sarama.ConsumerMessage {
	ch := make(chan *sarama.ConsumerMessage, len(c.messages))
	for _, message := range c.messages {
		ch <- message
	}
	close(ch)
	return ch
}

type Client struct {
	consumeErr error
	targetErr  error
	closeErr   error
	message    *sarama.ConsumerMessage
	t          *testing.T

	readMessages int
}

func (c *Client) Pause(partitions map[string][]int32) {
	//TODO implement me
	panic("implement me")
}

func (c *Client) Resume(partitions map[string][]int32) {
	//TODO implement me
	panic("implement me")
}

func (c *Client) PauseAll() {
	//TODO implement me
	panic("implement me")
}

func (c *Client) ResumeAll() {
	//TODO implement me
	panic("implement me")
}

func (c *Client) Errors() <-chan error {
	return nil
}

func (c *Client) Close() error {
	return c.closeErr
}

func (c *Client) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if handler == nil || c.targetErr != nil {
		handler = &consumer{
			concurrentWrites: 15,
			throttle:         make(chan struct{}, 15),
			source: &sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
				return c.targetErr
			}},
			log: log.WithFields(log.Fields{
				"source": "kafka",
			}),
		}
	}
	handler.Setup(nil)

	err := handler.ConsumeClaim(&sessionMock{}, claimMock{messages: []*sarama.ConsumerMessage{{
		Headers:        nil,
		Timestamp:      time.Now().UTC(),
		BlockTimestamp: time.Now().UTC(),
		Key:            bytes.NewBufferString(`testKey`).Bytes(),
		Value:          bytes.NewBufferString(uuid.NewV4().String()).Bytes(),
		Topic:          "testTopic",
		Partition:      0,
		Offset:         0,
	}}})
	if err != nil {
		return err
	}

	err = c.Close()
	if err != nil {
		return err
	}
	return nil
}

// initKafkaSource initializes a Kafka source with a mocked client
func initKafkaSource(t *testing.T, c *Config, targetErr, closeErr error) (*KafkaSource, error) {
	client := Client{
		targetErr:    targetErr,
		closeErr:     closeErr,
		t:            t,
		readMessages: 0,
	}
	s, err := NewKafkaSourceWithInterfaces(
		&client,
		&KafkaSource{
			config:           sarama.NewConfig(),
			concurrentWrites: 15,
			topic:            c.TopicName,
			brokers:          c.Brokers,
			consumerName:     c.ConsumerName,
			log: log.WithFields(log.Fields{
				"source":  "kafka",
				"brokers": c.Brokers,
				"topic":   c.TopicName,
			}),
		})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func TestKafkaSource_ReadSuccess(t *testing.T) {
	s, _ := initKafkaSource(t, &Config{
		Brokers:          "brokers:9092",
		ConcurrentWrites: 15,
		TopicName:        "testTopic",
		Assignor:         "range",
		TargetVersion:    sarama.SupportedVersions[0].String(),
		EnableSASL:       true,
		SASLUsername:     `Rob`,
		SASLPassword:     `robsPass`,
		SASLAlgorithm:    `sha512`,
	}, nil, nil)

	assert.NotNil(t, s.GetID())
	output := testutil.ReadAndReturnMessages(s, 3*time.Second, testutil.KafkaTestWriteBuilder, nil)
	assert.Equal(t, len(output), 100)
}

func TestKafkaSource_WriteToTargetError(t *testing.T) {
	s, _ := initKafkaSource(t, &Config{
		Brokers:          "brokers:9092",
		ConcurrentWrites: 15,
		TopicName:        "testTopic",
		Assignor:         "range",
		TargetVersion:    sarama.SupportedVersions[0].String(),
		EnableSASL:       true,
		SASLUsername:     `Rob`,
		SASLPassword:     `robsPass`,
		SASLAlgorithm:    `sha512`,
	}, errors.New(targetErr), nil)

	assert.NotNil(t, s.GetID())

	assert.PanicsWithError(t, targetErr, func() {
		testutil.ReadAndReturnMessages(s, 3*time.Second, testutil.KafkaTestWriteBuilder, nil)
	})
}

func TestKafkaSource_CloseErr(t *testing.T) {
	s, _ := initKafkaSource(t, &Config{
		Brokers:          "brokers:9092",
		ConcurrentWrites: 15,
		TopicName:        "testTopic",
		Assignor:         "range",
		TargetVersion:    sarama.SupportedVersions[0].String(),
		EnableSASL:       true,
		SASLUsername:     `Rob`,
		SASLPassword:     `robsPass`,
		SASLAlgorithm:    `sha512`,
	}, nil, errors.New(closeErr))

	assert.NotNil(t, s.GetID())

	assert.PanicsWithError(t, closeErr, func() {
		testutil.ReadAndReturnMessages(s, 3*time.Second, testutil.KafkaTestWriteBuilder, nil)
	})
}