// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

const (
	// Each message can only be up to 256 KB in size
	rabbitMQSendMessageByteLimit = 262144
)

// RabbitMQTargetConfig configures the destination for records consumed
type RabbitMQTargetConfig struct {
	ClusterURL string `hcl:"cluster_url" env:"TARGET_RABBITMQ_CLUSTER_URL"`
	Username   string `hcl:"username" env:"TARGET_RABBITMQ_USERNAME"`
	Password   string `hcl:"password" env:"TARGET_RABBITMQ_PASSWORD"`
	QueueName  string `hcl:"queue_name" env:"TARGET_RABBITMQ_QUEUE_NAME"`
}

// RabbitMQTarget holds a new client for writing messages to sqs
type RabbitMQTarget struct {
	clusterURL string
	username   string
	password   string
	queueName  string

	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue

	log *log.Entry
}

// newRabbitMQTarget creates a new client for writing messages to RabbitMQ
func newRabbitMQTarget(clusterURL string, username string, password string, queueName string) (*RabbitMQTarget, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", username, password, clusterURL))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect")
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to open a channel")
	}

	queue, err := channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to declare a queue")
	}

	return &RabbitMQTarget{
		clusterURL: clusterURL,
		username:   username,
		password:   password,
		queueName:  queueName,
		conn:       conn,
		channel:    channel,
		queue:      queue,
		log:        log.WithFields(log.Fields{"target": "rabbitmq", "queue": queueName}),
	}, nil
}

// RabbitMQTargetConfigFunction creates an RabbitMQTarget from an RabbitMQTargetConfig
func RabbitMQTargetConfigFunction(c *RabbitMQTargetConfig) (*RabbitMQTarget, error) {
	return newRabbitMQTarget(c.ClusterURL, c.Username, c.Password, c.QueueName)
}

// The RabbitMQTargetAdapter type is an adapter for functions to be used as
// pluggable components for RabbitMQ Target. It implements the Pluggable interface.
type RabbitMQTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f RabbitMQTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f RabbitMQTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults if any
	cfg := &RabbitMQTargetConfig{}

	return cfg, nil
}

// AdaptRabbitMQTargetFunc returns a RabbitMQTargetAdapter.
func AdaptRabbitMQTargetFunc(f func(c *RabbitMQTargetConfig) (*RabbitMQTarget, error)) RabbitMQTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*RabbitMQTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected RabbitMQTargetConfig")
		}

		return f(cfg)
	}
}

// Write pushes all messages to the required target
func (rs *RabbitMQTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	rs.log.Debugf("Writing %d messages to target queue ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		rs.MaximumAllowedMessageSizeBytes(),
	)

	var invalid []*models.Message
	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	for _, msg := range safeMessages {
		// Sent empty messages to invalid queue
		if len(msg.Data) == 0 {
			msg.SetError(errors.New("rabbitmq cannot accept empty messages: each message must contain either non-empty data, or at least one attribute"))
			invalid = append(invalid, msg)
			continue
		}

		// TODO: What settings are missing here?
		err := rs.channel.Publish(
			"",            // exchange
			rs.queue.Name, // routing key
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType: "text/plain",
				Body:        msg.Data,
			},
		)
		if err != nil {
			errResult = multierror.Append(errResult, err)

			failed = append(failed, msg)
		} else {
			if msg.AckFunc != nil {
				msg.AckFunc()
			}
			sent = append(sent, msg)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to RabbitMQ queue")
	}

	rs.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		invalid,
	), errResult
}

// Open does not do anything for this target
func (rs *RabbitMQTarget) Open() {}

// Close closes the connection to RabbitMQ
func (rs *RabbitMQTarget) Close() {
	rs.channel.Close()
	rs.conn.Close()
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (rs *RabbitMQTarget) MaximumAllowedMessageSizeBytes() int {
	return rabbitMQSendMessageByteLimit
}

// GetID returns the identifier for this target
func (rs *RabbitMQTarget) GetID() string {
	return fmt.Sprintf("%s", rs.queueName)
}
