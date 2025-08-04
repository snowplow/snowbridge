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

package pubsubsource

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	ProjectID                 string `hcl:"project_id"`
	SubscriptionID            string `hcl:"subscription_id"`
	ConcurrentWrites          int    `hcl:"concurrent_writes,optional"`
	MaxOutstandingMessages    int    `hcl:"max_outstanding_messages,optional"`
	MaxOutstandingBytes       int    `hcl:"max_outstanding_bytes,optional"`
	MinExtensionPeriodSeconds int    `hcl:"min_extension_period_seconds,optional"`
	StreamingPullGoRoutines   int    `hcl:"streaming_pull_goroutines,optional"`
	GRPCConnectionPool        int    `hcl:"grpc_connection_pool_size,optional"`
}

// pubSubSource holds a new client for reading messages from PubSub
type pubSubSource struct {
	projectID                 string
	client                    *pubsub.Client
	subscriptionID            string
	maxOutstandingMessages    int
	maxOutstandingBytes       int
	minExtensionPeriodSeconds int
	streamingPullGoRoutines   int

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
		c.MinExtensionPeriodSeconds,
		c.StreamingPullGoRoutines,
		c.GRPCConnectionPool,
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
		// ConcurrentWrites:          50,
		// Default is now handled in newPubsubSource, until we make a breaking release.
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    1e9,
		// StreamingPullGoRoutines:   1,
		// Similarly handled in newPubsubSource - when we make a breaking release this should be the default.
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
func newPubSubSource(concurrentWrites int, projectID string, subscriptionID string, maxOutstandingMessages, maxOutstandingBytes int, minExtensionPeriodSeconds int, streamingPullGoRoutines int, grpcConnectionPool int) (*pubSubSource, error) {
	ctx := context.Background()

	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	log := log.WithFields(log.Fields{"source": "pubsub", "cloud": "GCP", "project": projectID, "subscription": subscriptionID})

	// We use a slice to provide the grpcConnectionPool option only if it is set.
	// Otherwise we'll overwrite the client's clever under-the-hood default behaviour:
	// https://github.com/googleapis/google-cloud-go/blob/380e7d23e69b22ab46cc6e3be58902accee2f26a/pubsub/pubsub.go#L165-L177
	var opt []option.ClientOption
	if grpcConnectionPool != 0 {
		opt = append(opt, option.WithGRPCConnectionPool(grpcConnectionPool))
	}

	client, err := pubsub.NewClient(ctx, projectID, opt...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	// This temporary logic allows us to fix suboptimal behaviour without a breaking release.
	// The order of priority is streaming_pull_goroutines > concurrent_writes > previous default
	// We don't change the default because this would cause a major behaviour change in a non-major version bump

	// If streamingPullGoRoutines is not set but concurrentWrites is, use concurrentWrites.
	if streamingPullGoRoutines == 0 && concurrentWrites != 0 {
		streamingPullGoRoutines = concurrentWrites
		log.Warn("For the pubsub source, concurrent_writes is deprecated, and will be removed in the next major version. Use streaming_pull_goroutines instead")
	}
	// If neither are set, set it to the new default, but warn users of this behaviour change
	if streamingPullGoRoutines == 0 && concurrentWrites == 0 {
		streamingPullGoRoutines = 50
		log.Warn("Neither streaming_pull_goroutines nor concurrent_writes are set. The previous default is preserved, but strongly advise manual configuration of streaming_pull_goroutines, max_outstanding_messages and max_outstanding_bytes")
	}
	// Otherwise, streamingPullGoRoutines is set in the config and that value will be used.

	return &pubSubSource{
		projectID:                 projectID,
		client:                    client,
		subscriptionID:            subscriptionID,
		maxOutstandingMessages:    maxOutstandingMessages,
		maxOutstandingBytes:       maxOutstandingBytes,
		minExtensionPeriodSeconds: minExtensionPeriodSeconds,
		streamingPullGoRoutines:   streamingPullGoRoutines,
		log:                       log,
	}, nil
}

// Read will pull messages from the noted PubSub topic forever
func (ps *pubSubSource) Read(sf *sourceiface.SourceFunctions) error {
	ctx := context.Background()

	ps.log.Info("Reading messages from subscription ...")

	sub := ps.client.Subscription(ps.subscriptionID)
	sub.ReceiveSettings.NumGoroutines = ps.streamingPullGoRoutines         // This sets the number of goroutines that can open a streaming pull at once
	sub.ReceiveSettings.MaxOutstandingMessages = ps.maxOutstandingMessages // maxOutstandingMessages limits the number of messages processed at once (each spawns a goroutine)
	sub.ReceiveSettings.MaxOutstandingBytes = ps.maxOutstandingBytes
	sub.ReceiveSettings.MinExtensionPeriod = time.Duration(ps.minExtensionPeriodSeconds) * time.Second

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
