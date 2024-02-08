//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package target

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
)

// KafkaConfig contains configurable options for the kafka target
type KafkaConfig struct {
	Brokers        string `hcl:"brokers" env:"TARGET_KAFKA_BROKERS"`
	TopicName      string `hcl:"topic_name" env:"TARGET_KAFKA_TOPIC_NAME"`
	TargetVersion  string `hcl:"target_version,optional" env:"TARGET_KAFKA_TARGET_VERSION"`
	MaxRetries     int    `hcl:"max_retries,optional" env:"TARGET_KAFKA_MAX_RETRIES"`
	ByteLimit      int    `hcl:"byte_limit,optional" env:"TARGET_KAFKA_BYTE_LIMIT"`
	Compress       bool   `hcl:"compress,optional" env:"TARGET_KAFKA_COMPRESS"`
	WaitForAll     bool   `hcl:"wait_for_all,optional" env:"TARGET_KAFKA_WAIT_FOR_ALL"`
	Idempotent     bool   `hcl:"idempotent,optional" env:"TARGET_KAFKA_IDEMPOTENT"`
	EnableSASL     bool   `hcl:"enable_sasl,optional" env:"TARGET_KAFKA_ENABLE_SASL"`
	SASLUsername   string `hcl:"sasl_username,optional" env:"TARGET_KAFKA_SASL_USERNAME" `
	SASLPassword   string `hcl:"sasl_password,optional" env:"TARGET_KAFKA_SASL_PASSWORD"`
	SASLAlgorithm  string `hcl:"sasl_algorithm,optional" env:"TARGET_KAFKA_SASL_ALGORITHM"`
	CertFile       string `hcl:"cert_file,optional" env:"TARGET_KAFKA_TLS_CERT_FILE"`
	KeyFile        string `hcl:"key_file,optional" env:"TARGET_KAFKA_TLS_KEY_FILE"`
	CaFile         string `hcl:"ca_file,optional" env:"TARGET_KAFKA_TLS_CA_FILE"`
	SkipVerifyTLS  bool   `hcl:"skip_verify_tls,optional" env:"TARGET_KAFKA_TLS_SKIP_VERIFY_TLS"`
	ForceSync      bool   `hcl:"force_sync_producer,optional" env:"TARGET_KAFKA_FORCE_SYNC_PRODUCER"`
	FlushFrequency int    `hcl:"flush_frequency,optional" env:"TARGET_KAFKA_FLUSH_FREQUENCY"`
	FlushMessages  int    `hcl:"flush_messages,optional" env:"TARGET_KAFKA_FLUSH_MESSAGES"`
	FlushBytes     int    `hcl:"flush_bytes,optional" env:"TARGET_KAFKA_FLUSH_BYTES"`
}

// KafkaTarget holds a new client for writing messages to Apache Kafka
type KafkaTarget struct {
	syncProducer     sarama.SyncProducer
	asyncProducer    sarama.AsyncProducer
	asyncResults     chan *saramaResult
	topicName        string
	brokers          string
	messageByteLimit int

	log *log.Entry
}

// saramaResult holds the result of a Sarama request
type saramaResult struct {
	Msg *sarama.ProducerMessage
	Err error
}

// NewKafkaTarget creates a new client for writing messages to Apache Kafka
func NewKafkaTarget(cfg *KafkaConfig) (*KafkaTarget, error) {
	kafkaVersion, err := common.GetKafkaVersion(cfg.TargetVersion)
	if err != nil {
		return nil, err
	}

	logger := log.WithFields(log.Fields{"target": "kafka", "brokers": cfg.Brokers, "topic": cfg.TopicName, "version": kafkaVersion})
	sarama.Logger = logger

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "Snowbridge"
	saramaConfig.Version = kafkaVersion
	saramaConfig.Producer.Retry.Max = cfg.MaxRetries
	saramaConfig.Producer.MaxMessageBytes = cfg.ByteLimit

	// Must be enabled for the SyncProducer
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	if cfg.WaitForAll {
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	}

	if cfg.Idempotent {
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
		saramaConfig.Producer.Idempotent = true
		saramaConfig.Net.MaxOpenRequests = 1
	}

	if cfg.Compress {
		saramaConfig.Producer.Compression = sarama.CompressionSnappy // Compress messages
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

	var asyncResults chan *saramaResult = nil
	var asyncProducer sarama.AsyncProducer = nil
	var syncProducer sarama.SyncProducer = nil
	var producerError error = nil

	// If we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	if !cfg.ForceSync {
		saramaConfig.Producer.Flush.Messages = cfg.FlushMessages
		saramaConfig.Producer.Flush.Bytes = cfg.FlushBytes
		saramaConfig.Producer.Flush.Frequency = time.Duration(cfg.FlushFrequency) * time.Millisecond
	}

	// On the broker side, you may want to change the following settings to get stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	if !cfg.ForceSync {
		asyncProducer, producerError = sarama.NewAsyncProducer(strings.Split(cfg.Brokers, ","), saramaConfig)
		if producerError != nil {
			return nil, producerError
		}

		asyncResults = make(chan *saramaResult)

		go func() {
			for err := range asyncProducer.Errors() {
				asyncResults <- &saramaResult{Msg: err.Msg, Err: err.Err}
			}
		}()

		go func() {
			for success := range asyncProducer.Successes() {
				asyncResults <- &saramaResult{Msg: success}
			}
		}()
	} else {
		syncProducer, producerError = sarama.NewSyncProducer(strings.Split(cfg.Brokers, ","), saramaConfig)
	}

	return &KafkaTarget{
		syncProducer:     syncProducer,
		asyncProducer:    asyncProducer,
		asyncResults:     asyncResults,
		brokers:          cfg.Brokers,
		topicName:        cfg.TopicName,
		messageByteLimit: cfg.ByteLimit,
		log:              logger,
	}, producerError
}

// The KafkaTargetAdapter type is an adapter for functions to be used as
// pluggable components for Kafka target. It implements the Pluggable interface.
type KafkaTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f KafkaTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f KafkaTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &KafkaConfig{
		MaxRetries:    10,
		ByteLimit:     1048576,
		SASLAlgorithm: "sha512",
	}

	return cfg, nil
}

// AdaptKafkaTargetFunc returns a KafkaTargetAdapter.
func AdaptKafkaTargetFunc(f func(c *KafkaConfig) (*KafkaTarget, error)) KafkaTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*KafkaConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaConfig")
		}

		return f(cfg)
	}
}

// Write pushes all messages to the required target
func (kt *KafkaTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to topic ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		kt.MaximumAllowedMessageSizeBytes(),
	)

	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	if kt.asyncProducer != nil {
		// Not adding request latency metric to async producer for now, since it would complicate the implementation, and delay our debug.
		for _, msg := range safeMessages {
			kt.asyncProducer.Input() <- &sarama.ProducerMessage{
				Topic:    kt.topicName,
				Key:      sarama.StringEncoder(msg.PartitionKey),
				Value:    sarama.ByteEncoder(msg.Data),
				Metadata: msg,
			}
		}

		for i := 0; i < len(safeMessages); i++ {

			result := <-kt.asyncResults // Block until result is returned

			if result.Err != nil {
				errResult = multierror.Append(errResult, result.Err)
				originalMessage := result.Msg.Metadata.(*models.Message)
				originalMessage.SetError(result.Err)
				failed = append(failed, originalMessage)
			} else {
				originalMessage := result.Msg.Metadata.(*models.Message)
				if originalMessage.AckFunc != nil {
					originalMessage.AckFunc()
				}
				sent = append(sent, originalMessage)
			}
		}
	} else if kt.syncProducer != nil {
		for _, msg := range safeMessages {
			requestStarted := time.Now()
			_, _, err := kt.syncProducer.SendMessage(&sarama.ProducerMessage{
				Topic: kt.topicName,
				Key:   sarama.StringEncoder(msg.PartitionKey),
				Value: sarama.ByteEncoder(msg.Data),
			})
			requestFinished := time.Now()

			msg.TimeRequestStarted = requestStarted
			msg.TimeRequestFinished = requestFinished

			if err != nil {
				errResult = multierror.Append(errResult, err)
				msg.SetError(err)
				failed = append(failed, msg)
			} else {
				if msg.AckFunc != nil {
					msg.AckFunc()
				}
				sent = append(sent, msg)
			}
		}
	} else {
		errResult = multierror.Append(errResult, fmt.Errorf("no producer has been configured"))
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, fmt.Sprintf("Error writing messages to Kafka topic: %v", kt.topicName))
	}

	kt.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		nil,
	), errResult
}

// Open does not do anything for this target
func (kt *KafkaTarget) Open() {}

// Close stops the producer
func (kt *KafkaTarget) Close() {
	kt.log.Warnf("Closing Kafka target for topic '%s'", kt.topicName)

	if kt.asyncProducer != nil {
		if err := kt.asyncProducer.Close(); err != nil {
			kt.log.Fatal("Failed to close producer:", err)
		}
	}

	if kt.syncProducer != nil {
		if err := kt.syncProducer.Close(); err != nil {
			kt.log.Fatal("Failed to close producer:", err)
		}
	}
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (kt *KafkaTarget) MaximumAllowedMessageSizeBytes() int {
	return kt.messageByteLimit
}

// GetID returns the identifier for this target
func (kt *KafkaTarget) GetID() string {
	return fmt.Sprintf("brokers:%s:topic:%s", kt.brokers, kt.topicName)
}
