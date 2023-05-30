//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package kafkasource

import (
	"bytes"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/testutil"

	"github.com/Shopify/sarama"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
	"github.com/stretchr/testify/assert"
)

const (
	closeErr = `close error`
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

func (s sessionMock) MarkOffset(string, int32, int64, string) {}

func (s sessionMock) Commit() {}

func (s sessionMock) ResetOffset(string, int32, int64, string) {}

func (s sessionMock) MarkMessage(*sarama.ConsumerMessage, string) {}

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
}

func (c Client) Errors() <-chan error {
	return nil
}

func (c Client) Close() error {
	return c.closeErr
}

func (c Client) Pause(map[string][]int32) {}

func (c Client) Resume(map[string][]int32) {}

func (c Client) PauseAll() {}

func (c Client) ResumeAll() {}

func (c Client) Consume(_ context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	if handler == nil || c.targetErr != nil {
		handler = &consumer{
			concurrentWrites: 15,
			throttle:         make(chan struct{}, 15),
			source: &sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
				assert.Equal(c.t, messages[0].Data, c.message.Value)
				return c.targetErr
			}},
			log: log.WithFields(log.Fields{
				"source": "kafka",
			}),
		}
	}
	require.NoError(c.t, handler.Setup(nil))
	err := handler.ConsumeClaim(&sessionMock{}, claimMock{messages: []*sarama.ConsumerMessage{c.message}})
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
func initKafkaSource(t *testing.T, c *Configuration, targetErr, closeErr error) (*kafkaSource, error) {
	client := Client{
		targetErr: targetErr,
		closeErr:  closeErr,
		t:         t,
	}
	client.message = &sarama.ConsumerMessage{
		Headers:        nil,
		Timestamp:      time.Now().UTC(),
		BlockTimestamp: time.Now().UTC(),
		Key:            bytes.NewBufferString(`testKey`).Bytes(),
		Value:          bytes.NewBufferString(`testValue`).Bytes(),
		Topic:          "testTopic",
		Partition:      0,
		Offset:         0,
	}
	s, err := newKafkaSourceWithInterfaces(
		client,
		&kafkaSource{
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

func TestKafkaSource_CloseErr(t *testing.T) {
	s, _ := initKafkaSource(t, &Configuration{
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

	assert.PanicsWithError(t, closeErr, func() { testutil.ReadAndReturnMessages(s, 3*time.Second, testutil.DefaultTestWriteBuilder, nil) })
}
