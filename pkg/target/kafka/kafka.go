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

package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const SupportedTargetKafka = "kafka"

// KafkaConfig contains configurable options for the kafka target
type KafkaConfig struct {
	BatchingConfig *targetiface.BatchingConfig `hcl:"batching,block"`
	Brokers        string                      `hcl:"brokers"`
	TopicName      string                      `hcl:"topic_name"`
	TargetVersion  string                      `hcl:"target_version,optional"`
	MaxRetries     int                         `hcl:"max_retries,optional"`
	Compress       bool                        `hcl:"compress,optional"`
	WaitForAll     bool                        `hcl:"wait_for_all,optional"`
	Idempotent     bool                        `hcl:"idempotent,optional"`
	EnableSASL     bool                        `hcl:"enable_sasl,optional"`
	SASLUsername   string                      `hcl:"sasl_username,optional"`
	SASLPassword   string                      `hcl:"sasl_password,optional"`
	SASLAlgorithm  string                      `hcl:"sasl_algorithm,optional"`
	SASLVersion    int16                       `hcl:"sasl_version,optional"`
	EnableTLS      bool                        `hcl:"enable_tls,optional"`
	CertFile       string                      `hcl:"cert_file,optional"`
	KeyFile        string                      `hcl:"key_file,optional"`
	CaFile         string                      `hcl:"ca_file,optional"`
	SkipVerifyTLS  bool                        `hcl:"skip_verify_tls,optional"`
	ForceSync      bool                        `hcl:"force_sync_producer,optional"`
}

// KafkaTargetDriver holds a new client for writing messages to Apache Kafka
type KafkaTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
	syncProducer   sarama.SyncProducer
	asyncProducer  sarama.AsyncProducer
	asyncResults   chan *saramaResult
	topicName      string
	brokers        string

	log *log.Entry
}

// saramaResult holds the result of a Sarama request
type saramaResult struct {
	Msg *sarama.ProducerMessage
	Err error
}

// GetDefaultConfiguration returns the default configuration for Kafka target
func (kt *KafkaTargetDriver) GetDefaultConfiguration() any {
	return &KafkaConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     100,
			MaxBatchBytes:        1048576,
			MaxMessageBytes:      1048576,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
		MaxRetries:    5,
		SASLAlgorithm: "sha512",
		EnableTLS:     false,
	}
}

func (kt *KafkaTargetDriver) SetBatchingConfig(batchingConfig targetiface.BatchingConfig) {
	kt.BatchingConfig = batchingConfig
}

func (kt *KafkaTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return kt.BatchingConfig
}

// BuildKafkaFromConfig creates a Kafka target from decoded configuration
func (kt *KafkaTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*KafkaConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	// Set the batching config - used in both the below and the batcher.
	kt.SetBatchingConfig(*cfg.BatchingConfig)

	kafkaVersion, err := common.GetKafkaVersion(cfg.TargetVersion)
	if err != nil {
		return err
	}

	logger := log.WithFields(log.Fields{"target": SupportedTargetKafka, "brokers": cfg.Brokers, "topic": cfg.TopicName, "version": kafkaVersion})
	sarama.Logger = logger

	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "Snowbridge"
	saramaConfig.Version = kafkaVersion
	saramaConfig.Producer.Retry.Max = cfg.MaxRetries
	saramaConfig.Producer.MaxMessageBytes = kt.BatchingConfig.MaxMessageBytes

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
			cfg.SASLVersion,
		)
		if err != nil {
			return fmt.Errorf("failed to configure SASL, %w", err)
		}

		// Disable ApiVersionsRequest if using SASL v0 (incompatible)
		if cfg.SASLVersion == 0 {
			saramaConfig.ApiVersionsRequest = false
		}
	}

	// returns nil if certs are empty
	tlsConfig, err := common.CreateTLSConfiguration(cfg.CertFile, cfg.KeyFile, cfg.CaFile, cfg.SkipVerifyTLS)
	if err != nil {
		return err
	}
	saramaConfig.Net.TLS.Enable = cfg.EnableTLS
	saramaConfig.Net.TLS.Config = tlsConfig

	var asyncResults chan *saramaResult = nil
	var asyncProducer sarama.AsyncProducer = nil
	var syncProducer sarama.SyncProducer = nil
	var producerError error

	// If we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	if !cfg.ForceSync {
		saramaConfig.Producer.Flush.Messages = kt.BatchingConfig.MaxBatchMessages
		saramaConfig.Producer.Flush.Bytes = kt.BatchingConfig.MaxBatchBytes
		saramaConfig.Producer.Flush.Frequency = time.Duration(10) * time.Millisecond
	}

	// On the broker side, you may want to change the following settings to get stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	if !cfg.ForceSync {
		asyncProducer, producerError = sarama.NewAsyncProducer(strings.Split(cfg.Brokers, ","), saramaConfig)
		if producerError != nil {
			return producerError
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

	kt.syncProducer = syncProducer
	kt.asyncProducer = asyncProducer
	kt.asyncResults = asyncResults
	kt.brokers = cfg.Brokers
	kt.topicName = cfg.TopicName
	kt.log = logger

	return producerError
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (kt *KafkaTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, kt.BatchingConfig)
}

// Write pushes all messages to the required target
func (kt *KafkaTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	kt.log.Debugf("Writing %d messages to topic ...", len(messages))

	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	if kt.asyncProducer != nil {

		requestStarted := time.Now().UTC()
		for _, msg := range messages {
			kt.asyncProducer.Input() <- &sarama.ProducerMessage{
				Topic:    kt.topicName,
				Key:      sarama.StringEncoder(msg.PartitionKey),
				Value:    sarama.ByteEncoder(msg.Data),
				Metadata: msg,
			}
		}

		for i := 0; i < len(messages); i++ {
			result := <-kt.asyncResults // Block until result is returned

			originalMessage := result.Msg.Metadata.(*models.Message)
			originalMessage.TimeRequestStarted = requestStarted
			originalMessage.TimeRequestFinished = time.Now().UTC()

			if result.Err != nil {
				errResult = multierror.Append(errResult, result.Err)
				originalMessage.SetError(result.Err)
				failed = append(failed, originalMessage)
			} else {

				if originalMessage.AckFunc != nil {
					originalMessage.AckFunc()
				}
				sent = append(sent, originalMessage)
			}
		}
	} else if kt.syncProducer != nil {
		for _, msg := range messages {
			requestStarted := time.Now().UTC()
			_, _, err := kt.syncProducer.SendMessage(&sarama.ProducerMessage{
				Topic: kt.topicName,
				Key:   sarama.StringEncoder(msg.PartitionKey),
				Value: sarama.ByteEncoder(msg.Data),
			})
			requestFinished := time.Now().UTC()

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

	kt.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		nil,
		nil,
	), errResult
}

// Open does not do anything for this target
func (kt *KafkaTargetDriver) Open() error {
	return nil
}

// Close stops the producer
func (kt *KafkaTargetDriver) Close() {
	kt.log.Warnf("Closing Kafka target for topic '%s'", kt.topicName)

	if kt.asyncProducer != nil {
		if err := kt.asyncProducer.Close(); err != nil {
			kt.log.WithError(err).Error("Failed to close producer")
		}
	}

	if kt.syncProducer != nil {
		if err := kt.syncProducer.Close(); err != nil {
			kt.log.WithError(err).Error("Failed to close producer")
		}
	}
}
