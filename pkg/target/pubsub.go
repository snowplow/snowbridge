// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

const (
	// API Documentation: https://cloud.google.com/pubsub/quotas

	// Each record can only be up to 10 MiB in size
	pubSubPublishMessageByteLimit = 10485760
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
	Message *models.Message
}

// NewPubSubTarget creates a new client for writing messages to Google PubSub
func NewPubSubTarget(projectID string, topicName string) (*PubSubTarget, error) {
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
	ps.log.Debugf("Writing %d messages to topic ...", len(messages))

	ctx := context.Background()

	if ps.topic == nil {
		err := errors.New("Topic has not been opened, must call Open() before attempting to write")
		failed := messages

		return models.NewTargetWriteResult(
			nil,
			failed,
			nil,
			nil,
		), err
	}

	var results []*PubSubPublishResult

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		ps.MaximumAllowedMessageSizeBytes(),
	)

	var invalid []*models.Message

	for _, msg := range safeMessages {
		// Sent empty messages to invalid queue
		if len(msg.Data) == 0 {
			msg.SetError(errors.New("pubsub cannot accept empty messages: each message must contain either non-empty data, or at least one attribute"))
			invalid = append(invalid, msg)
			continue
		}

		pubSubMsg := &pubsub.Message{
			Data: msg.Data,
		}

		r := ps.topic.Publish(ctx, pubSubMsg)
		results = append(results, &PubSubPublishResult{
			Result:  r,
			Message: msg,
		})
	}

	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		if err != nil {
			errResult = multierror.Append(errResult, err)

			failed = append(failed, r.Message)
		} else {
			if r.Message.AckFunc != nil {
				r.Message.AckFunc()
			}

			sent = append(sent, r.Message)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to PubSub topic")
	}

	ps.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		invalid,
	), errResult
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

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (ps *PubSubTarget) MaximumAllowedMessageSizeBytes() int {
	return pubSubPublishMessageByteLimit
}

// GetID returns the identifier for this target
func (ps *PubSubTarget) GetID() string {
	return fmt.Sprintf("projects/%s/topics/%s", ps.projectID, ps.topicName)
}
