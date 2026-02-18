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
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

const SupportedSourceKafka = "kafka"

// Configuration configures the source for records
type Configuration struct {
	Brokers        string `hcl:"brokers"`
	TopicName      string `hcl:"topic_name"`
	ConsumerName   string `hcl:"consumer_name"`
	OffsetsInitial int64  `hcl:"offsets_initial"`

	Assignor      string `hcl:"assignor,optional"`
	TargetVersion string `hcl:"target_version,optional"`
	EnableSASL    bool   `hcl:"enable_sasl,optional"`
	SASLUsername  string `hcl:"sasl_username,optional" `
	SASLPassword  string `hcl:"sasl_password,optional"`
	SASLAlgorithm string `hcl:"sasl_algorithm,optional"`
	SASLVersion   int16  `hcl:"sasl_version,optional"`
	EnableTLS     bool   `hcl:"enable_tls,optional"`
	CertFile      string `hcl:"cert_file,optional"`
	KeyFile       string `hcl:"key_file,optional"`
	CaFile        string `hcl:"ca_file,optional"`
	SkipVerifyTLS bool   `hcl:"skip_verify_tls,optional"`
}

// DefaultConfiguration returns the default configuration for kafka source
func DefaultConfiguration() Configuration {
	return Configuration{
		Assignor:      "range",
		SASLAlgorithm: "sha512",
		EnableTLS:     false,
	}
}

// BuildFromConfig creates a kafka source from decoded configuration
func BuildFromConfig(cfg *Configuration) (sourceiface.Source, error) {
	kafkaVersion, err := common.GetKafkaVersion(cfg.TargetVersion)
	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{
		"source":  SupportedSourceKafka,
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
			sarama.NewBalanceStrategySticky(),
		}
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.NewBalanceStrategyRoundRobin(),
		}
	default:
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
			sarama.NewBalanceStrategyRange(),
		}
	}

	if cfg.EnableSASL {
		saramaConfig.Net.SASL, err = common.ConfigureSASL(
			cfg.SASLAlgorithm,
			cfg.SASLUsername,
			cfg.SASLPassword,
			cfg.SASLVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL, %w", err)
		}

		// Disable ApiVersionsRequest if using SASL v0 (incompatible)
		if cfg.SASLVersion == 0 {
			saramaConfig.ApiVersionsRequest = false
		}
	}

	// returns nil, nil if provided empty certs
	tlsConfig, err := common.CreateTLSConfiguration(cfg.CertFile, cfg.KeyFile, cfg.CaFile, cfg.SkipVerifyTLS)
	if err != nil {
		return nil, err
	}

	saramaConfig.Net.TLS.Enable = cfg.EnableTLS
	saramaConfig.Net.TLS.Config = tlsConfig

	sConfig := lazySaramaConfig{
		brokers: strings.Split(cfg.Brokers, ","),
		groupID: fmt.Sprintf(`%s-%s`, cfg.ConsumerName, cfg.TopicName),
		config:  saramaConfig,
	}

	return BuildWithSaramaConsumerInterface(nil, &kafkaSourceDriver{
		brokers:      cfg.Brokers,
		topic:        cfg.TopicName,
		consumerName: cfg.ConsumerName,
		log:          logger,
		saramaConfig: sConfig,
	})
}

// BuildWithSaramaConsumerInterface creates a new source for reading messages from Apache Kafka, allowing the user to provide a mocked client.
func BuildWithSaramaConsumerInterface(client sarama.ConsumerGroup, s *kafkaSourceDriver) (sourceiface.Source, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
	s.client = client
	return s, nil
}

// lazySaramaConfig holds Sarama consumer group config for lazy initialisation
type lazySaramaConfig struct {
	brokers []string
	groupID string
	config  *sarama.Config
}

// kafkaSourceDriver holds a new client for reading messages from Apache Kafka
type kafkaSourceDriver struct {
	sourceiface.SourceChannels

	topic        string
	brokers      string
	consumerName string
	log          *log.Entry

	saramaConfig lazySaramaConfig
	client       sarama.ConsumerGroup
}

// consumer represents a Sarama consumer group consumer
type consumer struct {
	outputChannel chan<- *models.Message
	log           *log.Entry
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
	// Create a local sequencer for this partition.
	// Each ConsumeClaim invocation processes one partition, so the sequencer
	// only needs to exist for this invocation's lifetime.
	sequencer := newKafkaOffsetSequencer()

	for message := range claim.Messages() {
		consumer.log.Debugf("Read message with key: %s", string(message.Key))

		newMessage := &models.Message{
			Data:         message.Value,
			PartitionKey: uuid.New().String(),
			TimeCreated:  message.Timestamp,
			TimePulled:   time.Now().UTC(),
		}
		if session != nil {
			// Create the sequenced ack function that will enforce ordering
			sequencedAckFn := sequencer.createSequencedAck(session, message)

			newMessage.AckFunc = func() {
				consumer.log.Debugf("Ack'ing message with Key: %s, Offset: %d", message.Key, message.Offset)

				// Call sequencer asynchronously to avoid blocking the calling thread.
				// Downstream concurrent transformation (e.g., with multiple transformer workers in the pool)
				// may cause messages to be reordered and then acked out of original order by targets,
				// but the sequencer ensures offsets are marked sequentially via channel-based ordering.
				// Similar to Kinsumer's approach: https://github.com/snowplow-devops/kinsumer/blob/v1.7.0/checkpoints.go#L274-L298
				go sequencedAckFn()
			}
		}

		select {
		case <-session.Context().Done():
			return nil
		case consumer.outputChannel <- newMessage:
		}

	}

	return nil
}

// Start initializes the Kafka consumer group and starts the message consumption loop
func (ks *kafkaSourceDriver) Start(ctx context.Context) {
	defer func() {
		close(ks.MessageChannel)

		if err := ks.client.Close(); err != nil {
			ks.log.WithError(err).Error("error closing kafka client")
		}
	}()

	ks.log.Info("Reading messages from topic...")

	// If client is nil
	if ks.client == nil {
		// then assumption is that we are planning to run against actual Kafka cluster,
		// as oppose to testing against stubbed sarama Consumer Group.
		// The reason to delay creation of actual consumer group until here is that NewConsumerGroup
		// attempts to connect to Kafka straightaway and thus we cannot call BuildFromConfig
		// without Kafka running and therefore cannot unit test BuildFromConfig function.
		client, err := sarama.NewConsumerGroup(ks.saramaConfig.brokers, ks.saramaConfig.groupID,
			ks.saramaConfig.config)
		if err != nil {
			ks.log.WithError(err).Error("Failed to create Kafka client")
			return
		}
		ks.client = client
	}

	consumer := consumer{
		outputChannel: ks.MessageChannel,
		log:           ks.log,
	}

	for {
		if err := ks.client.Consume(ctx, strings.Split(ks.topic, ","), &consumer); err != nil {
			ks.log.WithError(err).Error("Failed to consume from Kafka")
			break
		}
	}
}

// kafkaOffsetSequencer ensures offsets are committed sequentially even when acked out of order.
// Similar to Kinsumer's channel-based sequencing mechanism.
type kafkaOffsetSequencer struct {
	mutex       sync.Mutex
	lastChannel chan struct{}
}

// newKafkaOffsetSequencer creates a new offset sequencer with an initial closed channel.
func newKafkaOffsetSequencer() *kafkaOffsetSequencer {
	initialChannel := make(chan struct{})
	close(initialChannel) // First message can proceed immediately
	return &kafkaOffsetSequencer{
		lastChannel: initialChannel,
	}
}

// createSequencedAck creates an ack function that will execute sequentially.
// Even if acks are called out of order (e.g., msg 5, 2, 7), they will execute in order (2, 5, 7).
// This prevents Sarama's "highest offset wins" behavior from causing message loss.
//
// Similar to Kinsumer's updateFunc pattern:
// https://github.com/snowplow-devops/kinsumer/blob/v1.7.0/checkpoints.go#L274-L298
func (s *kafkaOffsetSequencer) createSequencedAck(
	session sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
) func() {
	s.mutex.Lock()
	prev := s.lastChannel
	next := make(chan struct{})
	s.lastChannel = next
	s.mutex.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			<-prev                       // Wait for previous message to be acked
			session.MarkMessage(msg, "") // Mark this message
			close(next)                  // Allow next message to be acked
		})
	}
}
