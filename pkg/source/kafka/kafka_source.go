// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kafka

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/xdg/scram"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
)

// KafkaSource holds a new client for reading messages from Apache Kafka
type KafkaSource struct {
	config       *sarama.Config
	client       *Client
	topic        string
	brokers      string
	consumerName string
	log          *log.Entry
	cancel       context.CancelFunc
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	source *sourceiface.SourceFunctions
	log    *log.Entry
}

type Client struct {
	Consume func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	Errors  func() <-chan error
	Close   func() error
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.log.Debugf("New session started")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	consumer.log.Debugf("Session ended, all ConsumeClaim goroutines exited")
	return nil
}

// ConsumeClaim claims consumed messages and writes them to the target
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var messages []*models.Message
		consumer.log.Debugf("Read message with key: %s", string(message.Key))

		messages = append(messages, &models.Message{
			Data:         message.Value,
			PartitionKey: uuid.NewV4().String(),
			TimeCreated:  message.Timestamp,
			TimePulled:   time.Now().UTC(),
		})
		if session != nil {
			session.MarkMessage(message, "")
		}

		consumer.source.WriteToTarget(messages)
	}

	return nil
}

// Read initializes the Kafka consumer group and starts the message consumption loop
func (ks *KafkaSource) Read(sf *sourceiface.SourceFunctions) error {
	// this allows mocking the client in unit tests
	if ks.client == nil {
		client, err := sarama.NewConsumerGroup(strings.Split(ks.brokers, ","), fmt.Sprintf(`%s-%s`, ks.consumerName, ks.topic), ks.config)
		if err != nil {
			log.Panicf("Error creating consumer group client: %v", err)
		}

		ks.client = &Client{
			Consume: client.Consume,
			Errors:  client.Errors,
			Close:   client.Close,
		}
	}

	consumer := Consumer{
		source: sf,
		log:    ks.log,
	}

	ctx, cancel := context.WithCancel(context.Background())
	// store reference to context cancel
	ks.cancel = cancel

	// start endless consumption loop
	for {
		if err := ks.client.Consume(ctx, strings.Split(ks.topic, ","), &consumer); err != nil {
			log.Println(ks.topic)
			log.Panicf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			break
		}
	}

	// close the client after loop has ended
	if err := ks.client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}

	return nil
}

// Stop cancels the source receiver
func (ks *KafkaSource) Stop() {
	if ks.cancel != nil {
		ks.log.Warn("Cancelling Kafka receiver...")
		ks.cancel()
	}
	ks.cancel = nil
}

// KafkaSourceConfigPair is passed to configuration to determine when to build a Kafka source.
var KafkaSourceConfigPair = sourceconfig.SourceConfigPair{SourceName: "kafka", SourceConfigFunc: KafkaSourceConfigFunction}

// KafkaSourceConfigFunction returns a kinesis source from a config
func KafkaSourceConfigFunction(c *config.Config) (sourceiface.Source, error) {
	return NewKafkaSource(c)
}

// NewKafkaSource creates a new source for reading messages from Apache Kafka
func NewKafkaSource(cfg *config.Config) (*KafkaSource, error) {
	kafkaVersion, err := getKafkaVersion(cfg.Sources.Kafka.TargetVersion)
	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{
		"source":  "kafka",
		"brokers": cfg.Sources.Kafka.Brokers,
		"topic":   cfg.Sources.Kafka.TopicName,
		"version": kafkaVersion})
	sarama.Logger = logger

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "snowplow_stream_replicator"
	saramaConfig.Version = kafkaVersion

	// Kafka rebalance strategy, defaulted to "range"
	switch cfg.Sources.Kafka.Assignor {
	case "sticky":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	default:
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	// validate SASL if enabled
	if cfg.Sources.Kafka.EnableSASL {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.Sources.Kafka.SASLUsername
		saramaConfig.Net.SASL.Password = cfg.Sources.Kafka.SASLPassword
		saramaConfig.Net.SASL.Handshake = true
		if cfg.Sources.Kafka.SASLAlgorithm == "sha512" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: SHA512} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if cfg.Sources.Kafka.SASLAlgorithm == "sha256" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: SHA256} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else if cfg.Sources.Kafka.SASLAlgorithm == "plaintext" {
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		} else {
			return nil, fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", cfg.Sources.Kafka.SASLAlgorithm)
		}
	}

	// validate TLS if required
	tlsConfig, err := target.CreateTLSConfiguration(cfg.Sources.Kafka.CertFile, cfg.Sources.Kafka.KeyFile, cfg.Sources.Kafka.CaFile, cfg.Sources.Kafka.SkipVerifyTLS)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		saramaConfig.Net.TLS.Config = tlsConfig
		saramaConfig.Net.TLS.Enable = true
	}

	return &KafkaSource{
		brokers:      cfg.Sources.Kafka.Brokers,
		topic:        cfg.Sources.Kafka.TopicName,
		consumerName: cfg.Sources.Kafka.ConsumerName,
		log:          logger,
	}, nil
}

// GetID returns the identifier for this target
func (ks *KafkaSource) GetID() string {
	return fmt.Sprintf("brokers:%s:topic:%s", ks.brokers, ks.topic)
}

func getKafkaVersion(targetVersion string) (sarama.KafkaVersion, error) {
	preferredVersion := sarama.DefaultVersion

	if targetVersion != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(targetVersion)
		if err != nil {
			return sarama.DefaultVersion, err
		}

		supportedVersion := false
		for _, version := range sarama.SupportedVersions {
			if version == parsedVersion {
				supportedVersion = true
				preferredVersion = parsedVersion
				break
			}
		}
		if !supportedVersion {
			return sarama.DefaultVersion, fmt.Errorf("unsupported version `%s`. select older, compatible version instead", parsedVersion)
		}
	}

	return preferredVersion, nil
}

// SHA256 hash
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 hash
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
