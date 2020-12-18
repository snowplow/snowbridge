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
	"sync"
)

// PubSubSource holds a new client for reading events from PubSub
type PubSubSource struct {
	ProjectID      string
	Client         *pubsub.Client
	SubscriptionID string
}

// NewPubSubSource creates a new client for reading events from PubSub
func NewPubSubSource(projectID string, subscriptionID string, serviceAccountB64 string) (*PubSubSource, error) {
	if serviceAccountB64 != "" {
		targetFile, err := storeGCPServiceAccountFromBase64(serviceAccountB64)
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
	}, nil
}

// Read will pull events from the noted PubSub topic up until the buffer limit
func (ps *PubSubSource) Read() ([]*Event, error) {
	ctx := context.Background()

	var mu sync.Mutex
	received := 0

	var events []*Event

	log.Infof("Reading records from subscription '%s' in project %s ...", ps.SubscriptionID, ps.ProjectID)

	sub := ps.Client.Subscription(ps.SubscriptionID)
	cctx, cancel := context.WithCancel(ctx)

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()

		// TODO: Move Ack to interface that triggers on successful write to avoid dropping data
		msg.Ack()

		// TODO: Attempt to get PartitionKey from attributes
		events = append(events, &Event{
			Data:         msg.Data,
			PartitionKey: uuid.NewV4().String(),
		})

		// TODO: Make buffer limit configurable
		received++
		if received == 10 {
			cancel()
		}
	})

	if err != nil {
		return nil, err
	}

	log.Infof("Successfully read %d records from subscription '%s' in project %s", len(events), ps.SubscriptionID, ps.ProjectID)

	return events, nil
}
