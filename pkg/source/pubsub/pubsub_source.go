// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package pubsubsource

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// configuration configures the source for records pulled
type configuration struct {
	ProjectID        string `hcl:"project_id" env:"SOURCE_PUBSUB_PROJECT_ID"`
	SubscriptionID   string `hcl:"subscription_id" env:"SOURCE_PUBSUB_SUBSCRIPTION_ID"`
	ConcurrentWrites int    `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}

// pubSubSource holds a new client for reading messages from PubSub
type pubSubSource struct {
	projectID        string
	client           *pubsub.Client
	subscriptionID   string
	concurrentWrites int

	log *log.Entry

	// cancel function to be used to halt reading
	cancel context.CancelFunc
}

// configFunction returns a pubsub source from a config
func configFunction(c *configuration) (sourceiface.Source, error) {
	return newPubSubSource(
		c.ConcurrentWrites,
		c.ProjectID,
		c.SubscriptionID,
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
	cfg := &configuration{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// adapterGenerator returns a PubSub Source adapter.
func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input, expected PubSubSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build a Pubsub source.
var ConfigPair = sourceconfig.ConfigPair{
	Name:   "pubsub",
	Handle: adapterGenerator(configFunction),
}

// newPubSubSource creates a new client for reading messages from PubSub
func newPubSubSource(concurrentWrites int, projectID string, subscriptionID string) (*pubSubSource, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	return &pubSubSource{
		projectID:        projectID,
		client:           client,
		subscriptionID:   subscriptionID,
		concurrentWrites: concurrentWrites,
		log:              log.WithFields(log.Fields{"source": "pubsub", "cloud": "GCP", "project": projectID, "subscription": subscriptionID}),
	}, nil
}

// Read will pull messages from the noted PubSub topic forever
func (ps *pubSubSource) Read(sf *sourceiface.SourceFunctions) error {
	ctx := context.Background()

	ps.log.Info("Reading messages from subscription ...")

	sub := ps.client.Subscription(ps.subscriptionID)
	sub.ReceiveSettings.NumGoroutines = ps.concurrentWrites

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
				PartitionKey: uuid.NewV4().String(),
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
