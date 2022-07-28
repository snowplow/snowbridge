// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.

package rabbitmqsource

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// configuration configures the source for records pulled
type configuration struct {
	ClusterURL       string `hcl:"cluster_url" env:"SOURCE_RABBITMQ_CLUSTER_URL"`
	Username         string `hcl:"username" env:"SOURCE_RABBITMQ_USERNAME"`
	Password         string `hcl:"password" env:"SOURCE_RABBITMQ_PASSWORD"`
	QueueName        string `hcl:"queue_name" env:"SOURCE_RABBITMQ_QUEUE_NAME"`
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}

// rabbitMQSource holds a new client for reading messages from RabbitMQ
type rabbitMQSource struct {
	clusterURL       string
	username         string
	password         string
	queueName        string
	concurrentWrites int

	log *log.Entry

	// exitSignal holds a channel for signalling an end to the read loop
	exitSignal chan struct{}
}

// configFunction returns a RabbitMQ source from a config
func configfunction(c *configuration) (sourceiface.Source, error) {
	return newRabbitMQSource(
		c.ClusterURL,
		c.Username,
		c.Password,
		c.QueueName,
		c.ConcurrentWrites,
	)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for RabbitMQ Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &configuration{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// adapterGenerator returns a RabbitMQSource adapter.
func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input, expected RabbitMQSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a RabbitMQ source.
var ConfigPair = sourceconfig.ConfigPair{
	Name:   "rabbitmq",
	Handle: adapterGenerator(configfunction),
}

// newRabbitMQSource creates a new client for reading messages from RabbitMQ
func newRabbitMQSource(clusterURL string, username string, password string, queueName string, concurrentWrites int) (*rabbitMQSource, error) {
	return &rabbitMQSource{
		clusterURL:       clusterURL,
		username:         username,
		password:         password,
		queueName:        queueName,
		concurrentWrites: concurrentWrites,
		log:              log.WithFields(log.Fields{"source": "rabbitmq", "queue": queueName}),
		exitSignal:       make(chan struct{}),
	}, nil
}

// Read will pull messages from the RabbitMQ queue forever until cancelled
func (rs *rabbitMQSource) Read(sf *sourceiface.SourceFunctions) error {
	rs.log.Info("Reading messages from queue ...")

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", rs.username, rs.password, rs.clusterURL))
	if err != nil {
		return errors.Wrap(err, "Failed to connect")
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrap(err, "Failed to open a channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		rs.queueName, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return errors.Wrap(err, "Failed to declare a queue")
	}
	err = ch.Qos(
	  rs.concurrentWrites, // prefetch count
	  0,                   // prefetch size
	  false,               // global
	)
	if err != nil {
		return errors.Wrap(err, "Failed to set QoS")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return errors.Wrap(err, "Failed to register a consumer")
	}

	throttle := make(chan struct{}, rs.concurrentWrites)
	wg := sync.WaitGroup{}

ProcessLoop:
	for {
		select {
		case <-rs.exitSignal:
			break ProcessLoop
		case d := <-msgs:
			throttle <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()

				timePulled := time.Now().UTC()

				rs.log.Debugf("Read message with ID: %s", d.MessageId)
				ackFunc := func() {
					rs.log.Debugf("Ack'ing message with ID: %s", d.MessageId)
					d.Ack(false)
				}

				timeCreated := d.Timestamp.UTC()
				messages := []*models.Message{
					{
						Data:         d.Body,
						PartitionKey: uuid.NewV4().String(),
						AckFunc:      ackFunc,
						TimeCreated:  timeCreated,
						TimePulled:   timePulled,
					},
				}

				err := sf.WriteToTarget(messages)
				if err != nil {
					rs.log.WithFields(log.Fields{"error": err}).Error(err)
				}
				<-throttle
			}()
		}
	}
	wg.Wait()

	return nil
}

// Stop will halt the reader processing more events
func (rs *rabbitMQSource) Stop() {
	rs.log.Warn("Cancelling RabbitMQ receive ...")
	rs.exitSignal <- struct{}{}
}

// GetID returns the identifier for this source
func (rs *rabbitMQSource) GetID() string {
	return fmt.Sprintf("%s", rs.queueName)
}
