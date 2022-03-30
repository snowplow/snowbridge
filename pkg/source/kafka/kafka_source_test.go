// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kafka

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/xdg/scram"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

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

// initKafkaSource initializes a Kafka source with a mocked client
func initKafkaSource(c *config.Config) (*KafkaSource, error) {
	s, err := NewKafkaSource(c)
	if err != nil {
		return nil, err
	}
	s.client = &Client{
		Consume: func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
			handler.Setup(nil)
			handler.ConsumeClaim(nil, claimMock{messages: []*sarama.ConsumerMessage{
				{
					Headers:        nil,
					Timestamp:      time.Now().UTC(),
					BlockTimestamp: time.Now().UTC(),
					Key:            bytes.NewBufferString(`testKey`).Bytes(),
					Value:          bytes.NewBufferString(`testValue`).Bytes(),
					Topic:          "testTopic",
					Partition:      0,
					Offset:         0,
				},
			}})
			_ = handler.Cleanup(nil)
			s.Stop()
			return nil
		},
		Errors: func() <-chan error {
			return nil
		},
		Close: func() error {
			return nil
		},
	}
	return s, nil
}

func TestKafkaSource_ReadSuccess(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `sha512`,
			},
		},
	})

	assert.NotNil(t, s.GetID())

	s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
		assert.NotNil(t, messages)
		return nil
	}})
}

func TestKafkaSource_TLSClient(t *testing.T) {
	x := xdgSCRAMClient{
		Client:             &scram.Client{},
		ClientConversation: nil,
		HashGeneratorFcn:   nil,
	}

	assert.Nil(t, x.Begin(`test`, `test`, `test`))
	_, err := x.Step(`challenge`)
	assert.Nil(t, err)
	assert.NotNil(t, x.Done())
}

func TestKafkaSource_AssignorSticky(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "sticky",
				TargetVersion: sarama.SupportedVersions[0].String(),
			},
		},
	})

	s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
		assert.NotNil(t, messages)
		return nil
	}})
}

func TestKafkaSource_AssignorRoundrobin(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "roundrobin",
				TargetVersion: sarama.SupportedVersions[0].String(),
			},
		},
	})

	s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
		assert.NotNil(t, messages)
		return nil
	}})
}

func TestKafkaSource_SASLAlgSHA256(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `sha256`,
			},
		},
	})

	s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
		assert.NotNil(t, messages)
		return nil
	}})
}

func TestKafkaSource_SASLAlgPlaintext(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `plaintext`,
			},
		},
	})

	s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
		assert.NotNil(t, messages)
		return nil
	}})
}

func TestKafkaSource_SASLAlgErr(t *testing.T) {
	_, err := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `wrongAlgorithm`,
			},
		},
	})

	assert.EqualError(t, err, `invalid SHA algorithm "wrongAlgorithm": can be either "sha256" or "sha512"`)
}

func TestKafkaSource_TLSErr(t *testing.T) {
	_, err := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				CertFile:      `certFile`,
				KeyFile:       `keyfile`,
			},
		},
	})

	assert.EqualError(t, err, `open certFile: no such file or directory`)
}

func TestKafkaSource_ClientErr(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `sha512`,
			},
		},
	})

	s.client = nil
	s.config = mocks.NewTestConfig()
	s.config.Net.MaxOpenRequests = -1

	assert.Panics(t, func() {
		s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
			assert.NotNil(t, messages)
			return nil
		}})
	})
}

func TestKafkaSource_ConsumeErr(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `sha512`,
			},
		},
	})

	s.client.Consume = func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		return errors.New(`consume error`)
	}

	assert.Panics(t, func() {
		s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
			assert.NotNil(t, messages)
			return nil
		}})
	})
}

func TestKafkaSource_CloseErr(t *testing.T) {
	s, _ := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: sarama.SupportedVersions[0].String(),
				EnableSASL:    true,
				SASLUsername:  `Rob`,
				SASLPassword:  `robsPass`,
				SASLAlgorithm: `sha512`,
			},
		},
	})

	s.client.Close = func() error {
		return errors.New(`close error`)
	}

	assert.Panics(t, func() {
		s.Read(&sourceiface.SourceFunctions{WriteToTarget: func(messages []*models.Message) error {
			assert.NotNil(t, messages)
			return nil
		}})
	})
}

func TestKafkaSource_VersionError(t *testing.T) {
	_, err := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: "incorrectness",
			},
		},
	})

	assert.EqualError(t, err, "invalid version `incorrectness`")
}

func TestKafkaSource_UnsupportedVersion(t *testing.T) {
	fmt.Println(sarama.SupportedVersions[0].String())
	_, err := initKafkaSource(&config.Config{
		Source: "kafka",
		Sources: config.SourcesConfig{
			Kafka: config.KafkaSourceConfig{
				Brokers:       "brokers:9092",
				TopicName:     "testTopic",
				Assignor:      "range",
				TargetVersion: "0.0.0.1",
			},
		},
	})

	assert.EqualError(t, err, "unsupported version `0.0.0.1`. select older, compatible version instead")
}
