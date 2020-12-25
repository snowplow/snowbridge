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
	"os"
	"strings"
)

// PubSubTarget holds a new client for writing events to Google PubSub
type PubSubTarget struct {
	ProjectID string
	Client    *pubsub.Client
	Topic     *pubsub.Topic
	TopicName string
}

// PubSubPublishResult contains the publish result and the function to execute
// on success to ack the send
type PubSubPublishResult struct {
	Result  *pubsub.PublishResult
	AckFunc func()
}

// NewPubSubTarget creates a new client for writing events to Google PubSub
func NewPubSubTarget(projectID string, topicName string, serviceAccountB64 string) (*PubSubTarget, error) {
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

	topic := client.Topic(topicName)

	return &PubSubTarget{
		ProjectID: projectID,
		Client:    client,
		Topic:     topic,
		TopicName: topicName,
	}, nil
}

// Write pushes all events to the required target
func (ps *PubSubTarget) Write(events []*Event) (*WriteResult, error) {
	ctx := context.Background()

	var results []*PubSubPublishResult

	log.Debugf("Writing %d messages to PubSub topic '%s' in project %s ...", len(events), ps.TopicName, ps.ProjectID)

	for _, event := range events {
		msg := &pubsub.Message{
			Data: event.Data,
		}

		r := ps.Topic.Publish(ctx, msg)
		results = append(results, &PubSubPublishResult{
			Result:  r,
			AckFunc: event.AckFunc,
		})
	}

	successes := 0
	failures := 0
	var errstrings []string

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		if err != nil {
			errstrings = append(errstrings, err.Error())
			failures++
		} else {
			successes++

			if r.AckFunc != nil {
				r.AckFunc()
			}
		}
	}

	var err error
	if len(errstrings) > 0 {
		err = fmt.Errorf(strings.Join(errstrings, "\n"))
	}

	log.Debugf("Successfully wrote %d/%d messages to PubSub topic '%s' in project %s", successes, len(events), ps.TopicName, ps.ProjectID)

	return &WriteResult{
		Sent:   int64(successes),
		Failed: int64(failures),
	}, err
}

// Close stops the topic
func (ps *PubSubTarget) Close() {
	log.Warnf("Closing PubSub target for topic '%s' in project %s", ps.TopicName, ps.ProjectID)
	ps.Topic.Stop()
}
