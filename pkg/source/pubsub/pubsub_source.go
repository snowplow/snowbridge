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
	"time"

	// nolint: staticcheck
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

const SupportedSourcePubsub = "pubsub"

// Configuration configures the source for records pulled
type Configuration struct {
	ProjectID                 string `hcl:"project_id"`
	SubscriptionID            string `hcl:"subscription_id"`
	MaxOutstandingMessages    int    `hcl:"max_outstanding_messages,optional"`
	MaxOutstandingBytes       int    `hcl:"max_outstanding_bytes,optional"`
	MinExtensionPeriodSeconds int    `hcl:"min_extension_period_seconds,optional"`
	StreamingPullGoRoutines   int    `hcl:"streaming_pull_goroutines,optional"`
	GRPCConnectionPool        int    `hcl:"grpc_connection_pool_size,optional"`
}

// pubSubSourceDriver holds a new client for reading messages from PubSub
type pubSubSourceDriver struct {
	sourceiface.SourceChannels

	projectID                 string
	client                    *pubsub.Client
	subscriptionID            string
	maxOutstandingMessages    int
	maxOutstandingBytes       int
	minExtensionPeriodSeconds int
	streamingPullGoRoutines   int

	log *log.Entry
}

// DefaultConfiguration returns the default configuration for pubsub source
func DefaultConfiguration() Configuration {
	return Configuration{
		MaxOutstandingMessages:  1000,
		MaxOutstandingBytes:     1e9,
		StreamingPullGoRoutines: 1,
	}
}

// BuildFromConfig creates a new client for reading messages from PubSub
func BuildFromConfig(cfg *Configuration) (sourceiface.Source, error) {
	ctx := context.Background()

	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	log := log.WithFields(log.Fields{"source": SupportedSourcePubsub, "cloud": "GCP", "project": cfg.ProjectID, "subscription": cfg.SubscriptionID})

	// We use a slice to provide the grpcConnectionPool option only if it is set.
	// Otherwise we'll overwrite the client's clever under-the-hood default behaviour:
	// https://github.com/googleapis/google-cloud-go/blob/380e7d23e69b22ab46cc6e3be58902accee2f26a/pubsub/pubsub.go#L165-L177
	var opt []option.ClientOption
	if cfg.GRPCConnectionPool != 0 {
		opt = append(opt, option.WithGRPCConnectionPool(cfg.GRPCConnectionPool))
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opt...)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	return &pubSubSourceDriver{
		projectID:                 cfg.ProjectID,
		client:                    client,
		subscriptionID:            cfg.SubscriptionID,
		maxOutstandingMessages:    cfg.MaxOutstandingMessages,
		maxOutstandingBytes:       cfg.MaxOutstandingBytes,
		minExtensionPeriodSeconds: cfg.MinExtensionPeriodSeconds,
		streamingPullGoRoutines:   cfg.StreamingPullGoRoutines,
		log:                       log,
	}, nil
}

// Start will pull messages from the noted PubSub topic
func (ps *pubSubSourceDriver) Start(ctx context.Context) {
	defer close(ps.MessageChannel)
	ps.log.Info("Reading messages from subscription ...")

	sub := ps.client.Subscription(ps.subscriptionID)
	sub.ReceiveSettings.NumGoroutines = ps.streamingPullGoRoutines         // This sets the number of goroutines that can open a streaming pull at once
	sub.ReceiveSettings.MaxOutstandingMessages = ps.maxOutstandingMessages // maxOutstandingMessages limits the number of messages processed at once (each spawns a goroutine)
	sub.ReceiveSettings.MaxOutstandingBytes = ps.maxOutstandingBytes
	sub.ReceiveSettings.MinExtensionPeriod = time.Duration(ps.minExtensionPeriodSeconds) * time.Second

	// Quote from receive docs: https://pkg.go.dev/cloud.google.com/go/pubsub#hdr-Receiving
	//
	// "Ack/Nack MUST be called within the Subscription.Receive handler function, and not from
	// a goroutine. Otherwise, flow control (e.g. ReceiveSettings.MaxOutstandingMessages) will
	// not be respected, and messages can get orphaned when cancelling Receive."

	// We write to output channel and then wait for ack/nack signal to ensure PubSub requirements are met.
	err := sub.Receive(ctx, func(msgCtx context.Context, msg *pubsub.Message) {
		shouldAckSignal := make(chan bool, 1)
		timePulled := time.Now().UTC()

		ps.log.Debugf("Read message with ID: %s", msg.ID)

		ackFunc := func() {
			shouldAckSignal <- true
		}

		nackFunc := func() {
			shouldAckSignal <- false
		}

		timeCreated := msg.PublishTime.UTC()
		message := &models.Message{
			Data:         msg.Data,
			PartitionKey: uuid.New().String(),
			AckFunc:      ackFunc,
			NackFunc:     nackFunc,
			TimeCreated:  timeCreated,
			TimePulled:   timePulled,
		}

		// Write to output channel if message's context is not cancelled yet.
		select {
		case <-msgCtx.Done():
			msg.Nack()
			return
		case ps.MessageChannel <- message:
		}

		// Block callback until processing complete.
		// This ensures Ack/Nack is called within the Pub/Sub callback handler as required
		shouldAck := <-shouldAckSignal
		if shouldAck {
			ps.log.Debugf("Ack'ing message with ID: %s", msg.ID)
			msg.Ack()
			return
		}

		ps.log.Debugf("Nack'ing message with ID: %s", msg.ID)
		msg.Nack()
	})

	if err != nil {
		ps.log.WithError(err).Error("Failed to read from PubSub topic")
	}
}
