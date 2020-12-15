// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"cloud.google.com/go/pubsub"
	"context"
	log "github.com/sirupsen/logrus"
)

// PubSubTarget holds a new client for writing events to Google PubSub
type PubSubTarget struct {
	ProjectID string
	Client    *pubsub.Client
	TopicName string
}

// NewPubSubTarget creates a new client for writing events to Google PubSub
func NewPubSubTarget(projectID string, topicName string) *PubSubTarget {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Panicf("FATAL: pubsub.NewClient: %s", err.Error())
	}

	return &PubSubTarget{
		ProjectID: projectID,
		Client:    client,
		TopicName: topicName,
	}
}

// Write pushes all events to the required target
func (ps *PubSubTarget) Write(events []*Event) error {
	ctx := context.Background()

	topic := ps.Client.Topic(ps.TopicName)
	defer topic.Stop()

	var results []*pubsub.PublishResult

	log.Infof("Writing %d records to target topic '%s' in project %s ...", len(events), ps.TopicName, ps.ProjectID)

	for _, event := range events {
		msg := &pubsub.Message{
			Data: event.Data,
		}

		r := topic.Publish(ctx, msg)
		results = append(results, r)
	}

	for _, r := range results {
		id, err := r.Get(ctx)

		// TODO: Accumulate failures instead of eagerly returning
		if err != nil {
			return err
		}

		log.Debugf("Published a message with message ID '%s' to topic '%s' in project %s", id, ps.TopicName, ps.ProjectID)
	}

	// TODO: Calculate successes and failures from above loop
	log.Infof("Successfully wrote %d records to target stream '%s' in project %s", len(events), ps.TopicName, ps.ProjectID)

	return nil
}
