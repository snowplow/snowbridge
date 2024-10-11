/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package pubsubsource

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	ProjectID              string `hcl:"project_id"`
	SubscriptionID         string `hcl:"subscription_id"`
	ConcurrentWrites       int    `hcl:"concurrent_writes,optional"`
	MaxOutstandingMessages int    `hcl:"max_outstanding_messages,optional"`
	MaxOutstandingBytes    int    `hcl:"max_outstanding_bytes,optional"`
}

// pubSubSource holds a new client for reading messages from PubSub
type pubSubSource struct {
	projectID              string
	client                 *pubsub.Client
	subscriptionID         string
	concurrentWrites       int
	maxOutstandingMessages int
	maxOutstandingBytes    int

	log *log.Entry

	// cancel function to be used to halt reading
	cancel context.CancelFunc
}

// configFunction returns a pubsub source from a config
func configFunction(c *Configuration) (sourceiface.Source, error) {
	return newPubSubSource(
		c.ConcurrentWrites,
		c.ProjectID,
		c.SubscriptionID,
		c.MaxOutstandingMessages,
		c.MaxOutstandingBytes,
	)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for PubSub Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Configuration{
		ConcurrentWrites:       50,
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    1e9,
	}

	return cfg, nil
}

// adapterGenerator returns a PubSub Source adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected PubSubSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Pubsub source.
var ConfigPair = config.ConfigurationPair{
	Name:   "pubsub",
	Handle: adapterGenerator(configFunction),
}

// newPubSubSource creates a new client for reading messages from PubSub
func newPubSubSource(concurrentWrites int, projectID string, subscriptionID string, maxOutstandingMessages, maxOutstandingBytes int) (*pubSubSource, error) {
	ctx := context.Background()

	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	return &pubSubSource{
		projectID:              projectID,
		client:                 client,
		subscriptionID:         subscriptionID,
		concurrentWrites:       concurrentWrites,
		maxOutstandingMessages: maxOutstandingMessages,
		maxOutstandingBytes:    maxOutstandingBytes,
		log:                    log.WithFields(log.Fields{"source": "pubsub", "cloud": "GCP", "project": projectID, "subscription": subscriptionID}),
	}, nil
}

// Read will pull messages from the noted PubSub topic forever
func (ps *pubSubSource) Read(sf *sourceiface.SourceFunctions) error {
	ctx := context.Background()

	ps.log.Info("Reading messages from subscription ...")

	sub := ps.client.Subscription(ps.subscriptionID)
	sub.ReceiveSettings.NumGoroutines = ps.concurrentWrites
	sub.ReceiveSettings.MaxOutstandingMessages = ps.maxOutstandingMessages
	sub.ReceiveSettings.MaxOutstandingBytes = ps.maxOutstandingBytes

	cctx, cancel := context.WithCancel(ctx)

	// Store reference to cancel
	ps.cancel = cancel

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		timePulled := time.Now().UTC()

		ps.log.Debugf("Read message with ID: %s", msg.ID)
		ackFunc := func() {
			ps.log.Debugf("Ack'ing message with ID: %s", msg.ID)
			msg.Ack()
		}

		timeCreated := msg.PublishTime.UTC()
		messages := []*models.Message{
			{
				Data:         msg.Data,
				PartitionKey: uuid.New().String(),
				AckFunc:      ackFunc,
				TimeCreated:  timeCreated,
				TimePulled:   timePulled,
			},
		}
		err := sf.WriteToTarget(messages)
		if err != nil {
			ps.log.WithFields(log.Fields{"error": err}).Error(err)
		}
	})

	if err != nil {
		return errors.Wrap(err, "Failed to read from PubSub topic")
	}
	return nil
}

// Stop will halt the reader processing more events
func (ps *pubSubSource) Stop() {
	if ps.cancel != nil {
		ps.log.Warn("Cancelling PubSub receive ...")
		ps.cancel()
	}
	ps.cancel = nil
}

// GetID returns the identifier for this source
func (ps *pubSubSource) GetID() string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", ps.projectID, ps.subscriptionID)
}
