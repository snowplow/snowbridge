// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"os"
	"time"
)

// PubSubSource holds a new client for reading messages from PubSub
type PubSubSource struct {
	ProjectID      string
	Client         *pubsub.Client
	SubscriptionID string
	log            *log.Entry

	// cancel function to be used to halt reading
	cancel context.CancelFunc
}

// NewPubSubSource creates a new client for reading messages from PubSub
func NewPubSubSource(projectID string, subscriptionID string, serviceAccountB64 string) (*PubSubSource, error) {
	if serviceAccountB64 != "" {
		targetFile, err := getGCPServiceAccountFromBase64(serviceAccountB64)
		if err != nil {
			return nil, err
		}
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", targetFile)
	}

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %s", err.Error())
	}

	return &PubSubSource{
		ProjectID:      projectID,
		Client:         client,
		SubscriptionID: subscriptionID,
		log:            log.WithFields(log.Fields{"name": "PubSubSource"}),
	}, nil
}

// Read will pull messages from the noted PubSub topic forever
func (ps *PubSubSource) Read(sf *SourceFunctions) error {
	ctx := context.Background()

	ps.log.Infof("Reading messages from subscription '%s' in project %s ...", ps.SubscriptionID, ps.ProjectID)

	sub := ps.Client.Subscription(ps.SubscriptionID)
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
		messages := []*Message{
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
			ps.log.Error(err)
		}
	})

	if err != nil {
		return err
	}
	return nil
}

// Stop will halt the reader processing more events
func (ps *PubSubSource) Stop() {
	if ps.cancel != nil {
		ps.log.Warn("Cancelling PubSub receive ...")
		ps.cancel()
	}
	ps.cancel = nil
}
