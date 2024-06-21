/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package kafkasource

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records
type Configuration struct {
	Brokers        string `hcl:"brokers"`
	TopicName      string `hcl:"topic_name"`
	ConsumerName   string `hcl:"consumer_name"`
	OffsetsInitial int64  `hcl:"offsets_initial"`

	ConcurrentWrites int    `hcl:"concurrent_writes,optional"`
	Assignor         string `hcl:"assignor,optional"`
	TargetVersion    string `hcl:"target_version,optional"`
	EnableSASL       bool   `hcl:"enable_sasl,optional"`
	SASLUsername     string `hcl:"sasl_username,optional" `
	SASLPassword     string `hcl:"sasl_password,optional"`
	SASLAlgorithm    string `hcl:"sasl_algorithm,optional"`
	CertFile         string `hcl:"cert_file,optional"`
	KeyFile          string `hcl:"key_file,optional"`
	CaFile           string `hcl:"ca_file,optional"`
	SkipVerifyTLS    bool   `hcl:"skip_verify_tls,optional"`
}

// kafkaSource holds a new client for reading messages from Apache Kafka
type kafkaSource struct {
	config           *sarama.Config
	concurrentWrites int
	topic            string
	brokers          string
	consumerName     string
	log              *log.Entry
	cancel           context.CancelFunc

	client sarama.ConsumerGroup
}

// consumer represents a Sarama consumer group consumer
type consumer struct {
	concurrentWrites int
	throttle         chan struct{}
	source           *sourceiface.SourceFunctions
	log              *log.Entry
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.log.Debugf("New session started")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim claims consumed messages and writes them to the target
func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	wg := sync.WaitGroup{}
	for message := range claim.Messages() {
		wg.Add(1)
		consumer.throttle <- struct{}{}
		go func(message *sarama.ConsumerMessage) {
			var messages []*models.Message
			consumer.log.Debugf("Read message with key: %s", string(message.Key))

			newMessage := &models.Message{
				Data:         message.Value,
				PartitionKey: uuid.New().String(),
				TimeCreated:  message.Timestamp,
				TimePulled:   time.Now().UTC(),
			}
			if session != nil {
				newMessage.AckFunc = func() {
					consumer.log.Debugf("Ack'ing message with Key: %s", message.Key)
					session.MarkMessage(message, "")
				}
			}

			messages = append(messages, newMessage)

			if err := consumer.source.WriteToTarget(messages); err != nil {
				// When WriteToTarget returns an error it just means we failed to send some data -
				// these messages won't have been acked, so they'll get retried eventually.
				consumer.log.WithFields(log.Fields{"error": err}).Error(err)
			}

			<-consumer.throttle
			wg.Done()
		}(message)
	}

	wg.Wait()

	return nil
}

// Read initializes the Kafka consumer group and starts the message consumption loop
func (ks *kafkaSource) Read(sf *sourceiface.SourceFunctions) error {
	ks.log.Info("Reading messages from topic...")

	consumer := consumer{
		throttle:         make(chan struct{}, ks.concurrentWrites),
		concurrentWrites: ks.concurrentWrites,
		source:           sf,
		log:              ks.log,
	}

	cctx, cancel := context.WithCancel(context.Background())
	// store reference to context cancel
	ks.cancel = cancel
	defer ks.client.Close()

	for {
		if err := ks.client.Consume(cctx, strings.Split(ks.topic, ","), &consumer); err != nil {
			return err
		}
		if ctxErr := cctx.Err(); ctxErr != nil {
			ks.log.WithFields(log.Fields{"error": ctxErr}).Error(ctxErr)
			// ignore this error, it is called by cancelled context (on application exit)
			return nil
		}
	}
}

// Stop cancels the source receiver
func (ks *kafkaSource) Stop() {
	if ks.cancel != nil {
		ks.log.Warn("Cancelling Kafka receiver...")
		ks.cancel()
	}
	ks.cancel = nil
}

// adapterGenerator returns a Kafka Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Kafka source.
var ConfigPair = config.ConfigurationPair{
	Name:   "kafka",
	Handle: adapterGenerator(configFunction),
}

// configFunction returns a kafka source from a config
func configFunction(c *Configuration) (sourceiface.Source, error) {
	return newKafkaSource(c)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for Kafka Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Configuration{
		Assignor:         "range",
		SASLAlgorithm:    "sha512",
		ConcurrentWrites: 15,
	}

	return cfg, nil
}

// newKafkaSource creates a new source for reading messages from Apache Kafka
func newKafkaSource(cfg *Configuration) (*kafkaSource, error) {
	kafkaVersion, err := common.GetKafkaVersion(cfg.TargetVersion)
	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{
		"source":  "kafka",
		"brokers": cfg.Brokers,
		"topic":   cfg.TopicName,
		"version": kafkaVersion,
	})
	sarama.Logger = logger

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "snowplow_snowbridge"
	saramaConfig.Version = kafkaVersion

	// -1 => OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.

	// -2 => OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	saramaConfig.Consumer.Offsets.Initial = cfg.OffsetsInitial

	// Kafka rebalance strategy, defaulted to "range"
	switch cfg.Assignor {
	case "sticky":
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.BalanceStrategySticky,
		}
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.BalanceStrategyRoundRobin,
		}
	default:
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.BalanceStrategyRange,
		}
	}

	if cfg.EnableSASL {
		saramaConfig.Net.SASL, err = common.ConfigureSASL(
			cfg.SASLAlgorithm,
			cfg.SASLUsername,
			cfg.SASLPassword,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL, %w", err)
		}
	}

	tlsConfig, err := common.CreateTLSConfiguration(cfg.CertFile, cfg.KeyFile, cfg.CaFile, cfg.SkipVerifyTLS)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		saramaConfig.Net.TLS.Config = tlsConfig
		saramaConfig.Net.TLS.Enable = true
	}

	client, err := sarama.NewConsumerGroup(strings.Split(cfg.Brokers, ","), fmt.Sprintf(`%s-%s`, cfg.ConsumerName, cfg.TopicName), saramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kafka client")
	}

	return newKafkaSourceWithInterfaces(client, &kafkaSource{
		brokers:          cfg.Brokers,
		topic:            cfg.TopicName,
		consumerName:     cfg.ConsumerName,
		log:              logger,
		concurrentWrites: cfg.ConcurrentWrites,
	})
}

// newKafkaSourceWithInterfaces creates a new source for reading messages from Apache Kafka, allowing the user to provide a mocked client.
func newKafkaSourceWithInterfaces(client sarama.ConsumerGroup, s *kafkaSource) (*kafkaSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
	s.client = client
	return s, nil
}

// GetID returns the identifier for this target
func (ks *kafkaSource) GetID() string {
	return fmt.Sprintf("brokers:%s:topic:%s", ks.brokers, ks.topic)
}
