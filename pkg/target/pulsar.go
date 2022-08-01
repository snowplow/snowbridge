// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// PulsarTargetConfig contains configurable options for the Pulsar target
type PulsarTargetConfig struct {
	BrokerServiceURL string `hcl:"broker_service_url" env:"TARGET_PULSAR_BROKER_URL"`
	TopicName        string `hcl:"topic_name" env:"TARGET_PULSAR_TOPIC_NAME"`
	ByteLimit        int `hcl:"byte_limit,optional" env:"TARGET_PULSAR_MESSAGE_BYTE_LIMIT"`
}

// PulsarTarget holds a new client for writing messages to Apache Pulsar
type PulsarTarget struct {
	client                  pulsar.Client
	producer                pulsar.Producer
	topicName               string
	brokerServiceURL        string
	maxConnectionsPerBroker int
	messageByteLimit        int

	log *log.Entry
}

// NewPulsarTarget creates a new client for writing messages to Apache Pulsar
func NewPulsarTarget(cfg *PulsarTargetConfig) (*PulsarTarget, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: cfg.BrokerServiceURL,
	})

	if err != nil {
		log.Fatal(err)
		return nil, errors.Wrap(err, "Failed to connect to pulsar broker service")
	}

	logger := log.WithFields(log.Fields{"target": "pulsar", "broker": cfg.BrokerServiceURL, "topic": cfg.TopicName})

	var maxConnectionsPerBroker = 1
	var producerError error = nil
	var producer pulsar.Producer = nil

	producer, producerError = client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.TopicName,
	})

	if producerError != nil {
		return nil, errors.Wrap(producerError, "Failed to create a producer")
	}

	return &PulsarTarget{
		producer:                producer,
		messageByteLimit:        cfg.ByteLimit,
		brokerServiceURL:        cfg.BrokerServiceURL,
		topicName:               cfg.TopicName,
		maxConnectionsPerBroker: maxConnectionsPerBroker,
		client:                  client,
		log:                     logger,
	}, producerError
}

// The PulsarTargetAdapter type is an adapter for functions to be used as
// pluggable components for Pulsar target. It implements the Pluggable interface.
type PulsarTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f PulsarTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f PulsarTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &PulsarTargetConfig{
		ByteLimit:     5242880,
	}

	return cfg, nil
}

// AdaptPulsarTargetFunc returns a PulsarTargetAdapter.
func AdaptPulsarTargetFunc(f func(c *PulsarTargetConfig) (*PulsarTarget, error)) PulsarTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*PulsarTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected PulsarTargetConfig")
		}

		return f(cfg)
	}
}

// Write pushes all messages to the required target
func (pt *PulsarTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	pt.log.Debugf("Writing %d messages to topic ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		pt.MaximumAllowedMessageSizeBytes(),
	)

	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	ctx := context.Background()

	if pt.producer != nil {
		for _, msg := range safeMessages {
			// Attempt to send the message
			_, err :=  pt.producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(fmt.Sprintf("%d", msg.String())),
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
	} else {
		errResult = multierror.Append(errResult, fmt.Errorf("no producer has been configured"))
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, fmt.Sprintf("Error writing messages to Pulsar topic: %v", pt.topicName))
	}

	pt.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		nil,
	), errResult
}

// Open does not do anything for this target
func (pt *PulsarTarget) Open() {}

// Close stops the producer
func (pt *PulsarTarget) Close() {
	pt.log.Warnf("Closing Pulsar target for topic '%s'", pt.topicName)

	if pt.producer != nil {
		pt.producer.Close();

		//if err := pt.producer.Close; err != nil {
		//	pt.log.Fatal("Failed to close producer:", err)
		//}
	}
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (pt *PulsarTarget) MaximumAllowedMessageSizeBytes() int {
	return pt.messageByteLimit
}

// GetID returns the identifier for this target
func (pt *PulsarTarget) GetID() string {
	return fmt.Sprintf("broker:%s:topic:%s", pt.brokerServiceURL, pt.topicName)
}
