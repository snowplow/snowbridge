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

const (
	// Each record can only be up to 1 MiB in size - Kafka default
	defaultKafkaMessageByteLimit = 1048576
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
func NewKafkaTarget(brokers string, topicName string, byteLimit int, idempotent bool, certFile string, keyFile string, caCert string, verifySsl bool) (*KafkaTarget, error) {
	brokerList := strings.Split(brokers, ",")

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()

	if idempotent {
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
	}

	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	tlsConfig := createTlsConfiguration(certFile, keyFile, caCert, verifySsl)
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	producer, err := sarama.NewSyncProducer(brokerList, config)

	return &KafkaTarget{
		producer:         producer,
		brokers:          brokers,
		topicName:        topicName,
		messageByteLimit: byteLimit,
		tlsConfig:        tlsConfig,
		log:              log.WithFields(log.Fields{"target": "kafka", "brokers": brokers, "topic": topicName}),
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

	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.
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

// Open opens a pipe to the topic
func (kt *KafkaTarget) Open() {
	kt.log.Warnf("Opening target for topic '%s'", kt.topicName)
}

// Close stops the topic
func (kt *KafkaTarget) Close() {
	kt.log.Warnf("Closing target for topic '%s'", kt.topicName)
	if err := kt.producer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (kt *KafkaTarget) MaximumAllowedMessageSizeBytes() int {
	if kt.messageByteLimit != 0 {
		return kt.messageByteLimit
	}

	return defaultKafkaMessageByteLimit
}

// GetID returns the identifier for this target
func (kt *KafkaTarget) GetID() string {
	return fmt.Sprintf("brokers:%s:topic:%s", kt.brokers, kt.topicName)
}

func createTlsConfiguration(certFile string, keyFile string, caCert string, verifySsl bool) (t *tls.Config) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}

		if caCert != "" {
			log.Fatal("No CA Cert provided but certFile and keyFile have been provided")
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(caCert))

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
