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
	"os/signal"
	"syscall"
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

// Read will pull events from the noted PubSub topic forever
func (ps *PubSubSource) Read(sf *SourceFunctions) error {
	ctx := context.Background()

	log.Infof("Reading messages from subscription '%s' in project %s ...", ps.SubscriptionID, ps.ProjectID)

	sub := ps.Client.Subscription(ps.SubscriptionID)
	cctx, cancel := context.WithCancel(ctx)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-sig
		log.Warn("SIGTERM called, cancelling PubSub receive ...")
		cancel()
	}()

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Debugf("Read message with ID: %s", msg.ID)
		ackFunc := func() {
			log.Debugf("Ack'ing message with ID: %s", msg.ID)
			msg.Ack()
		}

		events := []*Event{
			{
				Data:         msg.Data,
				PartitionKey: uuid.NewV4().String(),
				AckFunc:      ackFunc,
			},
		}
		err := sf.WriteToTarget(events)
		if err != nil {
			log.Error(err)
		}
	})

	sf.CloseTarget()

	if err != nil {
		return err
	}
	return nil
}
