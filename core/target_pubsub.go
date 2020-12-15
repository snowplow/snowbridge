// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"context"
	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

// PubSubTarget holds a new client for writing events to Google PubSub
type PubSubTarget struct {
	ProjectID string
	Client    *pubsub.Client
	Topic     *pubsub.Topic
	TopicName string
}

// NewPubSubTarget creates a new client for writing events to Google PubSub
func NewPubSubTarget(projectID string, topicName string) *PubSubTarget {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Panicf("FATAL: pubsub.NewClient: %s", err.Error())
	}
	topic := client.Topic(topicName)

	return &PubSubTarget{
		ProjectID: projectID,
		Client:    client,
		Topic:     topic,
		TopicName: topicName,
	}
}

// Write pushes all events to the required target
func (ps *PubSubTarget) Write(events []*Event) error {
	ctx := context.Background()
	
	log.Infof("Writing %d records to target topic '%s' in project %s ...", len(events), ps.TopicName, ps.ProjectID)

	for _, event := range events {
		msg := &pubsub.Message{
			Data: event.Data,
		}

		_, err := ps.Topic.Publish(ctx, msg).Get(ctx)
		if err != nil {
			return err
		}
	}

	log.Infof("Successfully wrote %d records to target stream '%s' in project %s", len(events), ps.TopicName, ps.ProjectID)

	return nil
}
