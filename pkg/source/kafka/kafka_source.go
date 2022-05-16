// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package kafkasource

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"strings"
	"sync"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/common"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"github.com/xdg/scram"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

const (
	kafkaSourceName = `kafka_source`
)

// configuration configures the source for records
type configuration struct {
	Brokers      string `hcl:"brokers" env:"SOURCE_KAFKA_BROKERS"`
	TopicName    string `hcl:"topic_name" env:"SOURCE_KAFKA_TOPIC_NAME"`
	ConsumerName string `hcl:"consumer_name" env:"SOURCE_KAFKA_CONSUMER_NAME"`

	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
	Assignor         string `hcl:"assignor,optional" env:"SOURCE_KAFKA_ASSIGNOR"`
	TargetVersion    string `hcl:"target_version,optional" env:"SOURCE_KAFKA_SOURCE_VERSION"`
	EnableSASL       bool   `hcl:"enable_sasl,optional" env:"SOURCE_KAFKA_ENABLE_SASL"`
	SASLUsername     string `hcl:"sasl_username,optional" env:"SOURCE_KAFKA_SASL_USERNAME" `
	SASLPassword     string `hcl:"sasl_password,optional" env:"SOURCE_KAFKA_SASL_PASSWORD"`
	SASLAlgorithm    string `hcl:"sasl_algorithm,optional" env:"SOURCE_KAFKA_SASL_ALGORITHM"`
	TLSCert          string `hcl:"tls_cert,optional" env:"SOURCE_KAFKA_TLS_CERT_B64"`
	TLSKey           string `hcl:"tls_key,optional" env:"SOURCE_KAFKA_TLS_KEY_B64"`
	TLSCa            string `hcl:"tls_ca,optional" env:"SOURCE_KAFKA_TLS_CA_B64"`
	SkipVerifyTLS    bool   `hcl:"skip_verify_tls,optional" env:"SOURCE_KAFKA_TLS_SKIP_VERIFY_TLS"`
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
	consumer.log.Debugf("Session ended, all ConsumeClaim goroutines exited")
	return nil
}

// ConsumeClaim claims consumed messages and writes them to the target
func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	wg := sync.WaitGroup{}
	var consumeErr error
	for message := range claim.Messages() {
		wg.Add(1)
		consumer.throttle <- struct{}{}
		go func(message *sarama.ConsumerMessage) {
			var messages []*models.Message
			consumer.log.Debugf("Read message with key: %s", string(message.Key))

			newMessage := &models.Message{
				Data:         message.Value,
				PartitionKey: uuid.NewV4().String(),
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

			err := consumer.source.WriteToTarget(messages)
			if err != nil {
				consumer.log.Debugf("Error writing to target: %s", err)
				consumeErr = err
			}

			<-consumer.throttle
			wg.Done()
		}(message)
	}

	wg.Wait()

	return consumeErr
}

// Read initializes the Kafka consumer group and starts the message consumption loop
func (ks *kafkaSource) Read(sf *sourceiface.SourceFunctions) error {
	consumer := consumer{
		throttle:         make(chan struct{}, ks.concurrentWrites),
		concurrentWrites: ks.concurrentWrites,
		source:           sf,
		log:              ks.log,
	}

	ctx, cancel := context.WithCancel(context.Background())
	// store reference to context cancel
	ks.cancel = cancel
	defer ks.client.Close()

	for {
		if err := ks.client.Consume(ctx, strings.Split(ks.topic, ","), &consumer); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
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
func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Kafka source.
var ConfigPair = sourceconfig.ConfigPair{
	Name:   "kafka",
	Handle: adapterGenerator(configFunction),
}

// configFunction returns a kafka source from a config
func configFunction(c *configuration) (sourceiface.Source, error) {
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
	cfg := &configuration{
		Assignor:         "range",
		SASLAlgorithm:    "sha512",
		ConcurrentWrites: 15,
	}

	return cfg, nil
}

// newKafkaSource creates a new source for reading messages from Apache Kafka
func newKafkaSource(cfg *configuration) (*kafkaSource, error) {
	kafkaVersion, err := getKafkaVersion(cfg.TargetVersion)
	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{
		"source":  "kafka",
		"brokers": cfg.Brokers,
		"topic":   cfg.TopicName,
		"version": kafkaVersion})
	sarama.Logger = logger

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "snowplow_stream_replicator"
	saramaConfig.Version = kafkaVersion

	// Kafka rebalance strategy, defaulted to "range"
	switch cfg.Assignor {
	case "sticky":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	default:
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	// validate SASL if enabled
	if cfg.EnableSASL {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.SASLUsername
		saramaConfig.Net.SASL.Password = cfg.SASLPassword
		saramaConfig.Net.SASL.Handshake = true
		if cfg.SASLAlgorithm == "sha512" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: SHA512} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if cfg.SASLAlgorithm == "sha256" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: SHA256} }
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else if cfg.SASLAlgorithm == "plaintext" {
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		} else {
			return nil, fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", cfg.SASLAlgorithm)
		}
	}

	// validate TLS if required
	tlsConfig, err := common.CreateTLSConfiguration(cfg.TLSCert, cfg.TLSKey, cfg.TLSCa, kafkaSourceName, cfg.SkipVerifyTLS)
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
	s.client = client
	return s, nil
}

// GetID returns the identifier for this target
func (ks *kafkaSource) GetID() string {
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
