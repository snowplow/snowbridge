// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// KafkaTarget holds a new client for writing messages to Apache Kafka
type KafkaTarget struct {
	producer  sarama.SyncProducer
	topicName string
	brokers   string

	messageByteLimit int

	tlsConfig *tls.Config

	log *log.Entry
}

// NewKafkaTarget creates a new client for writing messages to Apache Kafka
func NewKafkaTarget(brokers string, topicName string, version string, maxRetries int, byteLimit int, compress bool, waitForAll bool, idempotent bool, certFile string, keyFile string, caCert string, verifySsl bool) (*KafkaTarget, error) {
	preferredVersion := sarama.DefaultVersion

	if version != "" {
		preferredVersion, err := sarama.ParseKafkaVersion(version)
		if err != nil {
			return nil, err
		} else {
			supportedVersion := false
			for _, version := range sarama.SupportedVersions {
				if version == preferredVersion {
					supportedVersion = true
					break
				}
			}
			if !supportedVersion {
				return nil, fmt.Errorf("unsupported version `%s`. select older, compatible version instead", preferredVersion)
			}
		}
	}

	config := sarama.NewConfig()
	config.ClientID = "snowplow_stream_replicator"
	config.Version = preferredVersion
	config.Producer.Retry.Max = maxRetries
	config.Producer.MaxMessageBytes = byteLimit

	// Must be enabled for the SyncProducer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	if waitForAll {
		config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	}

	if idempotent {
		config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
	}

	// If we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	if compress {
		config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	}

	tlsConfig, err := createTlsConfiguration(certFile, keyFile, caCert, verifySsl)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	// On the broker side, you may want to change the following settings to get stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)

	return &KafkaTarget{
		producer:         producer,
		brokers:          brokers,
		topicName:        topicName,
		messageByteLimit: byteLimit,
		tlsConfig:        tlsConfig,
		log:              log.WithFields(log.Fields{"target": "kafka", "brokers": brokers, "topic": topicName, "version": preferredVersion}),
	}, err
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

	for _, msg := range safeMessages {
		_, _, err := kt.producer.SendMessage(&sarama.ProducerMessage{
			Topic: kt.topicName,
			Key:   sarama.StringEncoder(msg.PartitionKey),
			Value: sarama.ByteEncoder(msg.Data),
		})

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
	kt.log.Warnf("Closing target for topic '%s'", kt.topicName)
	if err := kt.producer.Close(); err != nil {
		kt.log.Fatal("Failed to close producer:", err)
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

func createTlsConfiguration(certFile string, keyFile string, caCert string, verifySsl bool) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	if caCert != "" {
		return nil, fmt.Errorf("tls: no caCert provided but certFile and keyFile have been provided")
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: verifySsl,
	}, nil
}
