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

// PubSubTarget holds a new client for writing messages to Google PubSub
type PubSubTarget struct {
	ProjectID string
	Client    *pubsub.Client
	Topic     *pubsub.Topic
	TopicName string
	log       *log.Entry
}

// PubSubPublishResult contains the publish result and the function to execute
// on success to ack the send
type PubSubPublishResult struct {
	Result  *pubsub.PublishResult
	AckFunc func()
}

// NewPubSubTarget creates a new client for writing messages to Google PubSub
func NewPubSubTarget(projectID string, topicName string, serviceAccountB64 string) (*PubSubTarget, error) {
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

	return &PubSubTarget{
		ProjectID: projectID,
		Client:    client,
		TopicName: topicName,
		log:       log.WithFields(log.Fields{"name": "PubSubTarget"}),
	}, nil
}

// Write pushes all messages to the required target
func (ps *PubSubTarget) Write(messages []*Message) (*TargetWriteResult, error) {
	ctx := context.Background()

	if ps.Topic == nil {
		return nil, fmt.Errorf("Topic has not been opened, must call Open() before attempting to write!")
	}

	var results []*PubSubPublishResult

	ps.log.Debugf("Writing %d messages to topic '%s' in project %s ...", len(messages), ps.TopicName, ps.ProjectID)

	for _, msg := range messages {
		pubSubMsg := &pubsub.Message{
			Data: msg.Data,
		}

		r := ps.Topic.Publish(ctx, pubSubMsg)
		results = append(results, &PubSubPublishResult{
			Result:  r,
			AckFunc: msg.AckFunc,
		})
	}

	sent := 0
	failed := 0
	var errstrings []string

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		if err != nil {
			errstrings = append(errstrings, err.Error())
			failed++
		} else {
			sent++

			if r.AckFunc != nil {
				r.AckFunc()
			}
		}
	}

	var err error
	if len(errstrings) > 0 {
		err = fmt.Errorf(strings.Join(errstrings, "\n"))
	}

	ps.log.Debugf("Successfully wrote %d/%d messages to topic '%s' in project %s", sent, len(messages), ps.TopicName, ps.ProjectID)
	return NewWriteResult(int64(sent), int64(failed), messages), err
}

// Open opens a pipe to the topic
func (ps *PubSubTarget) Open() {
	ps.log.Warnf("Opening target for topic '%s' in project %s", ps.TopicName, ps.ProjectID)
	ps.Topic = ps.Client.Topic(ps.TopicName)
}

// Close stops the topic
func (ps *PubSubTarget) Close() {
	ps.log.Warnf("Closing target for topic '%s' in project %s", ps.TopicName, ps.ProjectID)
	ps.Topic.Stop()
	ps.Topic = nil
}
