// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"

	"github.com/snowplow-devops/stream-replicator/internal/common"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

// PubSubTarget holds a new client for writing messages to Google PubSub
type PubSubTarget struct {
	projectID string
	client    *pubsub.Client
	topic     *pubsub.Topic
	topicName string

	log *log.Entry
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
		targetFile, err := common.GetGCPServiceAccountFromBase64(serviceAccountB64)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to store GCP Service Account JSON file")
		}
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", targetFile)
	}

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	return &PubSubTarget{
		projectID: projectID,
		client:    client,
		topicName: topicName,
		log:       log.WithFields(log.Fields{"target": "pubsub", "cloud": "GCP", "project": projectID, "topic": topicName}),
	}, nil
}

// Write pushes all messages to the required target
func (ps *PubSubTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ctx := context.Background()

	if ps.topic == nil {
		err := errors.New("Topic has not been opened, must call Open() before attempting to write")
		return models.NewWriteResult(int64(0), int64(len(messages)), messages), err
	}

	var results []*PubSubPublishResult

	ps.log.Debugf("Writing %d messages to topic ...", len(messages))

	for _, msg := range messages {
		pubSubMsg := &pubsub.Message{
			Data: msg.Data,
		}

		r := ps.topic.Publish(ctx, pubSubMsg)
		results = append(results, &PubSubPublishResult{
			Result:  r,
			AckFunc: msg.AckFunc,
		})
	}

	sent := 0
	failed := 0

	var errResult error

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		if err != nil {
			errResult = multierror.Append(errResult, err)
			failed++
		} else {
			sent++
			if r.AckFunc != nil {
				r.AckFunc()
			}
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to PubSub topic")
	}

	ps.log.Debugf("Successfully wrote %d/%d messages", sent, len(messages))
	return models.NewWriteResult(int64(sent), int64(failed), messages), errResult
}

// Open opens a pipe to the topic
func (ps *PubSubTarget) Open() {
	ps.log.Warnf("Opening target for topic '%s' in project %s", ps.topicName, ps.projectID)
	ps.topic = ps.client.Topic(ps.topicName)
}

// Close stops the topic
func (ps *PubSubTarget) Close() {
	ps.log.Warnf("Closing target for topic '%s' in project %s", ps.topicName, ps.projectID)
	ps.topic.Stop()
	ps.topic = nil
}
